package com.pr_reviewer.queueforge.broker;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Partition {

    private final String partitionDir;
    private final int maxMessagesPerSegment = 1000;
    private final int maxSegments = 5;

    private final TreeMap<Long, Integer> segmentMessageCounts = new TreeMap<>();

    private final Map<Long, Map<Long, Long>> loadedIndexes = new HashMap<>();
    private final Map<Long, FileChannel> openChannels = new HashMap<>();

    private long activeSegmentStartOffset;
    private FileChannel activeChannel;
    private long activeWritePosition;
    private int messagesInActiveSegment;
    private long nextOffset = 0;


    public Partition(String partitionDir) throws IOException {
        this.partitionDir = partitionDir;
        Files.createDirectories(Paths.get(partitionDir));

        if (loadMetadata()) {
            // start from metadata, load the last segment
            activeSegmentStartOffset = segmentMessageCounts.lastKey();
            messagesInActiveSegment = segmentMessageCounts.get(activeSegmentStartOffset);
            nextOffset = activeSegmentStartOffset + messagesInActiveSegment;
            activeChannel = openChannel(activeSegmentStartOffset);
            activeWritePosition = activeChannel.size();
            System.out.println("Loaded partition from metadata: " + segmentMessageCounts.size() + " segments, nextOffset=" + nextOffset);
        } else {
            // for new segment
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
                long start = Long.parseLong(parts[0]);
                int count = Integer.parseInt(parts[1]);
                segmentMessageCounts.put(start, count);
            }
            return !segmentMessageCounts.isEmpty();
        } catch (Exception e) {
            System.out.println("Metadata corrupt/unreadable, falling back to fresh state: " + e.getMessage());
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

    private String segmentPath(long startOffset) {
        return partitionDir + "/segment-" + startOffset + ".log";
    }

    private FileChannel openChannel(long startOffset) throws IOException {
        if (openChannels.containsKey(startOffset)) {
            return openChannels.get(startOffset);
        }
        FileChannel channel = FileChannel.open(Paths.get(segmentPath(startOffset)), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        openChannels.put(startOffset, channel);
        return channel;
    }

    private void startNewSegment(long startOffset) throws IOException {
        activeChannel = openChannel(startOffset);
        segmentMessageCounts.put(startOffset, 0);

        activeSegmentStartOffset = startOffset;
        activeWritePosition = 0;
        messagesInActiveSegment = 0;
    }

    private Map<Long, Long> loadIndexForSegment(long segStart) throws IOException {
        if (loadedIndexes.containsKey(segStart)) {
            return loadedIndexes.get(segStart);
        }

        FileChannel channel = openChannel(segStart);
        Map<Long, Long> offsetToPosition = new HashMap<>();

        long position = 0;
        long fileSize = channel.size();
        long offset = segStart;
        ByteBuffer lengthBuf = ByteBuffer.allocate(4);

        while (position < fileSize) {
            channel.position(position);
            lengthBuf.clear();
            channel.read(lengthBuf);
            lengthBuf.flip();
            int length = lengthBuf.getInt();

            offsetToPosition.put(offset, position);
            position += 4 + length;
            offset++;
        }

        loadedIndexes.put(segStart, offsetToPosition);
        return offsetToPosition;
    }

    public synchronized long append(byte[] value) throws IOException {
        if (messagesInActiveSegment >= maxMessagesPerSegment) {
            startNewSegment(nextOffset);
            enforceRetention();
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + value.length);
        buffer.putInt(value.length);
        buffer.put(value);
        buffer.flip();

        activeChannel.position(activeWritePosition);
        activeChannel.write(buffer);

        long offset = nextOffset;

        loadedIndexes.computeIfAbsent(activeSegmentStartOffset, k -> new HashMap<>())
                .put(offset, activeWritePosition);

        activeWritePosition += 4 + value.length;
        messagesInActiveSegment++;
        nextOffset++;

        segmentMessageCounts.put(activeSegmentStartOffset, messagesInActiveSegment);
        saveMetadata();

        return offset;
    }

    public synchronized byte[] read(long offset) throws IOException {
        Long segStart = segmentMessageCounts.floorKey(offset);
        if (segStart == null) return null;

        Map<Long, Long> offsetToPosition = loadIndexForSegment(segStart);
        Long position = offsetToPosition.get(offset);
        if (position == null) return null;

        FileChannel channel = openChannel(segStart);
        channel.position(position);

        ByteBuffer lengthBuf = ByteBuffer.allocate(4);
        channel.read(lengthBuf);
        lengthBuf.flip();
        int length = lengthBuf.getInt();

        ByteBuffer valueBuf = ByteBuffer.allocate(length);
        channel.read(valueBuf);
        valueBuf.flip();

        byte[] value = new byte[length];
        valueBuf.get(value);
        return value;
    }

    private void enforceRetention() throws IOException {
        while (segmentMessageCounts.size() > maxSegments) {
            long oldestStart = segmentMessageCounts.firstKey();

            FileChannel channel = openChannels.remove(oldestStart);
            if (channel != null) channel.close();
            loadedIndexes.remove(oldestStart);

            Files.deleteIfExists(Paths.get(segmentPath(oldestStart)));
            segmentMessageCounts.remove(oldestStart);

            System.out.println("Deleted old segment starting at offset " + oldestStart);
        }
        saveMetadata();
    }

    public long size() {
        return nextOffset;
    }

    public void close() throws IOException {
        for (FileChannel ch : openChannels.values()) {
            ch.close();
        }
    }





}
