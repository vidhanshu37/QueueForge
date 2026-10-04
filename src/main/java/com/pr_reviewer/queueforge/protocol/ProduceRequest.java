package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class ProduceRequest {
    public final String topic;
    public final int partition;
    public final byte[] key;
    public final byte[] value;
    public final int ack; // 0, 1, ya -1 (all)

    public ProduceRequest(String topic, int partition, byte[] key, byte[] value, int ack) {
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.ack = ack;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        byte[] topicBytes = topic.getBytes("UTF-8");
        out.writeInt(topicBytes.length);
        out.write(topicBytes);
        out.writeInt(partition);

        if (key == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(key.length);
            out.write(key);
        }

        out.writeInt(value.length);
        out.write(value);
        out.writeInt(ack);

        return baos.toByteArray();
    }

    public static ProduceRequest decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

        int topicLen = in.readInt();
        byte[] topicBytes = new byte[topicLen];
        in.readFully(topicBytes);
        String topic = new String(topicBytes, "UTF-8");

        int partition = in.readInt();

        int keyLen = in.readInt();
        byte[] key = null;
        if (keyLen >= 0) {
            key = new byte[keyLen];
            in.readFully(key);
        }

        int valueLen = in.readInt();
        byte[] value = new byte[valueLen];
        in.readFully(value);
        int ack = in.readInt();

        return new ProduceRequest(topic, partition, key, value, ack);
    }
}