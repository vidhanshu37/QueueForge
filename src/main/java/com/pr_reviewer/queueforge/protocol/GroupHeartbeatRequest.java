package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class GroupHeartbeatRequest {

    public final String groupId;
    public final String consumerId;
    public final int generation; // get when join

    public GroupHeartbeatRequest(String groupId, String consumerId, int generation) {
        this.groupId = groupId;
        this.consumerId = consumerId;
        this.generation = generation;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        byte[] g = groupId.getBytes("UTF-8");
        out.writeInt(g.length);
        out.write(g);
        byte[] c = consumerId.getBytes("UTF-8");
        out.writeInt(c.length);
        out.write(c);
        out.writeInt(generation);
        return baos.toByteArray();
    }

    public static GroupHeartbeatRequest decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        byte[] g = new byte[in.readInt()];
        in.readFully(g);
        byte[] c = new byte[in.readInt()];
        in.readFully(c);
        int generation = in.readInt();
        return new GroupHeartbeatRequest(new String(g, "UTF-8"), new String(c, "UTF-8"), generation);
    }
}
