package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class ProduceRequest {
    public final String topic;
    public final int partition;
    public final byte[] key;
    public final byte[] value;

    public ProduceRequest(String topic, int partition, byte[] key, byte[] value) {
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream arrOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(arrOut);

        byte[] topicBytes = topic.getBytes("UTF-8");
        dataOut.writeInt(topicBytes.length);
        dataOut.write(topicBytes);
        dataOut.writeInt(partition);

        if(key == null) {
            dataOut.writeInt(-1);
        } else {
            dataOut.writeInt(key.length);
            dataOut.write(key);
        }

        dataOut.writeInt(value.length);
        dataOut.write(value);

        return arrOut.toByteArray();
    }

    public static ProduceRequest decode(byte[] payload) throws IOException {
        DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(payload));

        int topicLength = dataIn.readInt();
        byte[] topicBytes = new byte[topicLength];
        dataIn.readFully(topicBytes);
        String topic = new String(topicBytes, "UTF-8");

        int partition = dataIn.readInt();

        int keyLength = dataIn.readInt();
        byte[] key = null;

        if(keyLength >= 0 ) {
            key = new byte[keyLength];
            dataIn.readFully(key);
        }

        int valueLength = dataIn.readInt();
        byte[] value = new byte[valueLength];
        dataIn.readFully(value);

        return  new ProduceRequest(topic, partition, key, value);
    }
}
