package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class OffsetCommitRequest {
    public final String groupId;
    public final String topic;
    public final int partition;
    public final long offset; // offset that to be going to read

    public OffsetCommitRequest(String groupId, String topic, int partition, long offset) {
        this.groupId = groupId;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String mapKey() { return groupId + "|" + topic + "|" + partition; }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        byte[] g = groupId.getBytes("UTF-8");
        out.writeInt(g.length);
        out.write(g);
        byte[] t = topic.getBytes("UTF-8");
        out.writeInt(t.length);
        out.write(t);
        out.writeInt(partition);
        out.writeLong(offset);
        return baos.toByteArray();
    }

    public static OffsetCommitRequest decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        byte[] g = new byte[in.readInt()];
        in.readFully(g);
        byte[] t = new byte[in.readInt()];
        in.readFully(t);
        int partition = in.readInt();
        long offset = in.readLong();
        return new OffsetCommitRequest(new String(g, "UTF-8"), new String(t, "UTF-8"), partition, offset);
    }



}
