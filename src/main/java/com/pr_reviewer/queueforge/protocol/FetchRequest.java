package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class FetchRequest {
    public final String topic;
    public final int partition;
    public final long offset;

    public FetchRequest(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        byte[] topicBytes = topic.getBytes("UTF-8");
        out.writeInt(topicBytes.length);
        out.write(topicBytes);
        out.writeInt(partition);
        out.writeLong(offset);

        return baos.toByteArray();
    }

    public static FetchRequest decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

        int topicLen = in.readInt();
        byte[] topicBytes = new byte[topicLen];
        in.readFully(topicBytes);
        String topic = new String(topicBytes, "UTF-8");

        int partition = in.readInt();
        long offset = in.readLong();

        return new FetchRequest(topic, partition, offset);
    }
}
