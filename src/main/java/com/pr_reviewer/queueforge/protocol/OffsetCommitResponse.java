package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class OffsetCommitResponse {
    public static final byte ERR_NONE = 0;
    public static final byte ERR_NOT_LEADER = 1;
    public static final byte ERR_NOT_ENOUGH_REPLICAS = 2;
    public static final byte ERR_NOT_READY = 3;

    public final byte errorCode;
    public final String leaderHint;

    public OffsetCommitResponse(byte errorCode, String leaderHint) {
        this.errorCode = errorCode; this.leaderHint = leaderHint;
    }

    public boolean success() { return errorCode == ERR_NONE; }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeByte(errorCode);
        byte[] h = (leaderHint == null ? "" : leaderHint).getBytes("UTF-8");
        out.writeInt(h.length); out.write(h);
        return baos.toByteArray();
    }

    public static OffsetCommitResponse decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        byte errorCode = in.readByte();
        byte[] h = new byte[in.readInt()]; in.readFully(h);
        String hint = new String(h, "UTF-8");
        return new OffsetCommitResponse(errorCode, hint.isEmpty() ? null : hint);
    }
}
