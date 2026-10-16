package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class ListOffsetsRequest {
    public final String topic;
    public final int partition;

    public ListOffsetsRequest(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        byte[] topicBytes = topic.getBytes("UTF-8");
        out.writeInt(topicBytes.length);
        out.write(topicBytes);
        out.writeInt(partition);
        return baos.toByteArray();
    }

    public static ListOffsetsRequest decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        int topicLen = in.readInt();
        byte[] topicBytes = new byte[topicLen];
        in.readFully(topicBytes);
        String topic = new String(topicBytes, "UTF-8");
        int partition = in.readInt();
        return new ListOffsetsRequest(topic, partition);
    }
}