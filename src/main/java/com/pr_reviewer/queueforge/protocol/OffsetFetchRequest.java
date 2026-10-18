package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class OffsetFetchRequest {
    public final String groupId;
    public final String topic;
    public final int partition;

    public OffsetFetchRequest(String groupId, String topic, int partition) {
        this.groupId = groupId; this.topic = topic; this.partition = partition;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        byte[] g = groupId.getBytes("UTF-8");
        out.writeInt(g.length); out.write(g);
        byte[] t = topic.getBytes("UTF-8");
        out.writeInt(t.length); out.write(t);
        out.writeInt(partition);
        return baos.toByteArray();
    }

    public static OffsetFetchRequest decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        byte[] g = new byte[in.readInt()]; in.readFully(g);
        byte[] t = new byte[in.readInt()]; in.readFully(t);
        int partition = in.readInt();
        return new OffsetFetchRequest(new String(g, "UTF-8"), new String(t, "UTF-8"), partition);
    }
}
