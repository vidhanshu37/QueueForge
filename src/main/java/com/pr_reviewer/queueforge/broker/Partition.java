package com.pr_reviewer.queueforge.broker;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Partition {
    private final FileChannel channel;
    private final Map<Long, Long> offsetToPosition = new ConcurrentHashMap<>();
    private long nextOffset = 0;
    private long writePosition = 0;

    public Partition(String logFilePath) throws IOException {
        Path path = Paths.get(logFilePath);
        boolean isNewFile = !Files.exists(path);

        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        if (!isNewFile) {
            recover();
        }
        writePosition = channel.size();
    }

    private void recover() throws IOException {
        long position = 0;
        long fileSize = channel.size();

        ByteBuffer lengthBuf = ByteBuffer.allocate(4);

        while (position < fileSize) {
            channel.position(position);

            lengthBuf.clear();
            channel.read(lengthBuf);
            lengthBuf.flip();
            int length = lengthBuf.getInt();

            offsetToPosition.put(nextOffset, position);

            position += 4 + length; // 4 bytes length-prefix + message bytes
            nextOffset++;
        }
        System.out.println("Recovered " + nextOffset + " messages from " + logFilePathSafe());
    }

    private String logFilePathSafe() {
        return "log file";
    }

    public synchronized long append(byte[] value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4 + value.length);
        buffer.putInt(value.length);
        buffer.put(value);
        buffer.flip();

        channel.position(writePosition);
        channel.write(buffer);

        long offset = nextOffset;
        offsetToPosition.put(offset, writePosition);

        writePosition += 4 + value.length;
        nextOffset++;

        return offset;
    }

    public synchronized byte[] read(long offset) throws IOException {
        Long position = offsetToPosition.get(offset);
        if (position == null) {
            return null;
        }

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

    public long size() {
        return nextOffset;
    }

    public void close() throws IOException {
        channel.close();
    }
}
