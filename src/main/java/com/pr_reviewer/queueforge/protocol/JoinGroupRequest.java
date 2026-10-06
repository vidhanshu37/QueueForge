package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class JoinGroupRequest {
    public final String groupId;
    public final String consumerId;
    public final String topic;

    public JoinGroupRequest(String groupId, String consumerId, String topic) {
        this.groupId = groupId;
        this.consumerId = consumerId;
        this.topic = topic;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        byte[] groupIdBytes = groupId.getBytes("UTF-8");
        out.writeInt(groupIdBytes.length);
        out.write(groupIdBytes);

        byte[] consumerIdBytes = consumerId.getBytes("UTF-8");
        out.writeInt(consumerIdBytes.length);
        out.write(consumerIdBytes);

        byte[] topicBytes = topic.getBytes("UTF-8");
        out.writeInt(topicBytes.length);
        out.write(topicBytes);

        return baos.toByteArray();
    }

    public static JoinGroupRequest decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

        int groupIdLen = in.readInt();
        byte[] groupIdBytes = new byte[groupIdLen];
        in.readFully(groupIdBytes);
        String groupId = new String(groupIdBytes, "UTF-8");

        int consumerIdLen = in.readInt();
        byte[] consumerIdBytes = new byte[consumerIdLen];
        in.readFully(consumerIdBytes);
        String consumerId = new String(consumerIdBytes, "UTF-8");

        int topicLen = in.readInt();
        byte[] topicBytes = new byte[topicLen];
        in.readFully(topicBytes);
        String topic = new String(topicBytes, "UTF-8");

        return new JoinGroupRequest(groupId, consumerId, topic);
    }
}
