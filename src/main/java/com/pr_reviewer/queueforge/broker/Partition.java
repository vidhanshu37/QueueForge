package com.pr_reviewer.queueforge.broker;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

public class Partition {

    private final String partitionDir;
    private final int maxMessagesPerSegment = 1000;
    private final int maxSegments = 5;
    private final int indexInterval = 100;

    private static final int INDEX_RECORD_SIZE = 16;

    private final TreeMap<Long, Integer> segmentMessageCounts = new TreeMap<>();
    private final Map<Long, FileChannel> logChannels = new HashMap<>();
    private final Map<Long, FileChannel> indexChannels = new HashMap<>();

    private long activeSegmentStartOffset;
    private FileChannel activeLogChannel;
    private FileChannel activeIndexChannel;
    private long activeWritePosition;
    private int messagesInActiveSegment;
    private long nextOffset = 0;

    public Partition(String partitionDir) throws IOException {
        this.partitionDir = partitionDir;
        Files.createDirectories(Paths.get(partitionDir));

        if (loadMetadata()) {
            activeSegmentStartOffset = segmentMessageCounts.lastKey();
            messagesInActiveSegment = segmentMessageCounts.get(activeSegmentStartOffset);
            nextOffset = activeSegmentStartOffset + messagesInActiveSegment;

            activeLogChannel = openLogChannel(activeSegmentStartOffset);
            activeIndexChannel = openIndexChannel(activeSegmentStartOffset);
            activeWritePosition = activeLogChannel.size();

            System.out.println("Loaded partition from metadata: " + segmentMessageCounts.size() +
                    " segments, nextOffset=" + nextOffset);
        } else {
            startNewSegment(0);
            saveMetadata();
        }
    }

    private Path metadataPath() {
        return Paths.get(partitionDir, "metadata.json");
    }

    private boolean loadMetadata() {
        Path path = metadataPath();
        if (!Files.exists(path)) return false;
        try {
            String content = new String(Files.readAllBytes(path), "UTF-8");
            if (content.isBlank()) return false;
            for (String entry : content.trim().split(",")) {
                String[] parts = entry.split(":");
                segmentMessageCounts.put(Long.parseLong(parts[0]), Integer.parseInt(parts[1]));
            }
            return !segmentMessageCounts.isEmpty();
        } catch (Exception e) {
            System.out.println("Metadata corrupt/unreadable, starting fresh: " + e.getMessage());
            segmentMessageCounts.clear();
            return false;
        }
    }


    private void saveMetadata() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, Integer> e : segmentMessageCounts.entrySet()) {
            if (sb.length() > 0) sb.append(",");
            sb.append(e.getKey()).append(":").append(e.getValue());
        }
        Path tempFile = Paths.get(partitionDir, "metadata.tmp");
        Files.write(tempFile, sb.toString().getBytes("UTF-8"));
        Files.move(tempFile, metadataPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private String logPath(long startOffset) {
        return partitionDir + "/segment-" + startOffset + ".log";
    }

    private String indexPath(long startOffset) {
        return partitionDir + "/segment-" + startOffset + ".index";
    }

    private FileChannel openLogChannel(long startOffset) throws IOException {
        return logChannels.computeIfAbsent(startOffset, s -> {
            try {
                return FileChannel.open(Paths.get(logPath(s)),
                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            } catch (IOException e) { throw new RuntimeException(e); }
        });
    }

    private FileChannel openIndexChannel(long startOffset) throws IOException {
        return indexChannels.computeIfAbsent(startOffset, s -> {
            try {
                return FileChannel.open(Paths.get(indexPath(s)),
                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            } catch (IOException e) { throw new RuntimeException(e); }
        });
    }

    private void startNewSegment(long startOffset) throws IOException {
        activeLogChannel = openLogChannel(startOffset);
        activeIndexChannel = openIndexChannel(startOffset);
        segmentMessageCounts.put(startOffset, 0);

        activeSegmentStartOffset = startOffset;
        activeWritePosition = 0;
        messagesInActiveSegment = 0;
    }

    private void appendIndexEntry(FileChannel indexChannel, long offset, long position) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(INDEX_RECORD_SIZE);
        buf.putLong(offset);
        buf.putLong(position);
        buf.flip();
        indexChannel.position(indexChannel.size());
        indexChannel.write(buf);
    }

    private long[] binarySearchFloor(FileChannel indexChannel, long targetOffset) throws IOException {
        long numEntries = indexChannel.size() / INDEX_RECORD_SIZE;
        if (numEntries == 0) return null;

        long lo = 0, hi = numEntries - 1;
        long[] result = null;
        ByteBuffer buf = ByteBuffer.allocate(INDEX_RECORD_SIZE);

        while (lo <= hi) {
            long mid = (lo + hi) / 2;
            buf.clear();
            indexChannel.position(mid * INDEX_RECORD_SIZE);
            indexChannel.read(buf);
            buf.flip();
            long entryOffset = buf.getLong();
            long entryPosition = buf.getLong();

            if (entryOffset <= targetOffset) {
                result = new long[]{entryOffset, entryPosition};
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result;
    }

    public synchronized long append(byte[] value) throws IOException {
        if (messagesInActiveSegment >= maxMessagesPerSegment) {
            startNewSegment(nextOffset);
            enforceRetention();
        }

        long offset = nextOffset;
        long position = activeWritePosition;

        ByteBuffer buffer = ByteBuffer.allocate(4 + value.length);
        buffer.putInt(value.length);
        buffer.put(value);
        buffer.flip();

        activeLogChannel.position(position);
        activeLogChannel.write(buffer);

        long relativeCount = offset - activeSegmentStartOffset;
        if (relativeCount % indexInterval == 0) {
            appendIndexEntry(activeIndexChannel, offset, position);
        }

        activeWritePosition += 4 + value.length;
        messagesInActiveSegment++;
        nextOffset++;

        segmentMessageCounts.put(activeSegmentStartOffset, messagesInActiveSegment);
        saveMetadata();

        return offset;
    }

    public synchronized byte[] read(long targetOffset) throws IOException {
        Long segStart = segmentMessageCounts.floorKey(targetOffset);
        if (segStart == null) return null;

        FileChannel logChannel = openLogChannel(segStart);
        FileChannel indexChannel = openIndexChannel(segStart);

        long[] floorEntry = binarySearchFloor(indexChannel, targetOffset);

        long scanOffset;
        long scanPosition;
        if (floorEntry == null) {
            scanOffset = segStart;
            scanPosition = 0;
        } else {
            scanOffset = floorEntry[0];
            scanPosition = floorEntry[1];
        }

        ByteBuffer lengthBuf = ByteBuffer.allocate(4);
        while (scanOffset < targetOffset) {
            logChannel.position(scanPosition);
            lengthBuf.clear();
            logChannel.read(lengthBuf);
            lengthBuf.flip();
            int length = lengthBuf.getInt();

            scanPosition += 4 + length;
            scanOffset++;
        }

        logChannel.position(scanPosition);
        lengthBuf.clear();
        logChannel.read(lengthBuf);
        lengthBuf.flip();
        int length = lengthBuf.getInt();

        ByteBuffer valueBuf = ByteBuffer.allocate(length);
        logChannel.read(valueBuf);
        valueBuf.flip();

        byte[] value = new byte[length];
        valueBuf.get(value);
        return value;
    }

    private void enforceRetention() throws IOException {
        while (segmentMessageCounts.size() > maxSegments) {
            long oldestStart = segmentMessageCounts.firstKey();

            FileChannel logCh = logChannels.remove(oldestStart);
            if (logCh != null) logCh.close();
            FileChannel idxCh = indexChannels.remove(oldestStart);
            if (idxCh != null) idxCh.close();

            Files.deleteIfExists(Paths.get(logPath(oldestStart)));
            Files.deleteIfExists(Paths.get(indexPath(oldestStart)));
            segmentMessageCounts.remove(oldestStart);

            System.out.println("Deleted old segment starting at offset " + oldestStart);
        }
        saveMetadata();
    }

    public long size() {
        return nextOffset;
    }

    public void close() throws IOException {
        for (FileChannel ch : logChannels.values()) ch.close();
        for (FileChannel ch : indexChannels.values()) ch.close();
    }
}