package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class ReplicateRequest {
    public final String topic;
    public final int partition;
    public final long offset;
    public final byte[] value;

    public ReplicateRequest(String topic, int partition, long offset, byte[] value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.value = value;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        byte[] topicBytes = topic.getBytes("UTF-8");
        out.writeInt(topicBytes.length);
        out.write(topicBytes);
        out.writeInt(partition);
        out.writeLong(offset);
        out.writeInt(value.length);
        out.write(value);

        return baos.toByteArray();
    }

    public static ReplicateRequest decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

        int topicLen = in.readInt();
        byte[] topicBytes = new byte[topicLen];
        in.readFully(topicBytes);
        String topic = new String(topicBytes, "UTF-8");

        int partition = in.readInt();
        long offset = in.readLong();
        int valueLen = in.readInt();
        byte[] value = new byte[valueLen];
        in.readFully(value);

        return new ReplicateRequest(topic, partition, offset, value);
    }
}
