package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class HeartbeatRequest {
    public final int term;
    public final String leaderId;

    public HeartbeatRequest(int term, String leaderId) {
        this.term = term;
        this.leaderId = leaderId;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(term);
        byte[] idBytes = leaderId.getBytes("UTF-8");
        out.writeInt(idBytes.length);
        out.write(idBytes);
        return baos.toByteArray();
    }

    public static HeartbeatRequest decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        int term = in.readInt();
        int idLen = in.readInt();
        byte[] idBytes = new byte[idLen];
        in.readFully(idBytes);
        return new HeartbeatRequest(term, new String(idBytes, "UTF-8"));
    }
}
