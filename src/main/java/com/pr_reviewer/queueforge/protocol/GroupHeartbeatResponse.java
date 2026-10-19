package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class GroupHeartbeatResponse {
    public static final byte ERR_NONE = 0;
    public static final byte ERR_NOT_LEADER = 1;
    public static final byte ERR_UNKNOWN_MEMBER = 2;
    public static final byte ERR_REBALANCE = 3;

    public final byte errorCode;
    public final int generation;
    public final String leaderHint;

    public GroupHeartbeatResponse(byte errorCode, int generation, String leaderHint) {
        this.errorCode = errorCode;
        this.generation = generation;
        this.leaderHint = leaderHint;
    }

    public boolean success() {
        return errorCode == ERR_NONE;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeByte(errorCode);
        out.writeInt(generation);
        byte[] h = (leaderHint == null ? "" : leaderHint).getBytes("UTF-8");
        out.writeInt(h.length);
        out.write(h);
        return baos.toByteArray();
    }

    public static GroupHeartbeatResponse decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        byte errorCode = in.readByte();
        int generation = in.readInt();
        byte[] h = new byte[in.readInt()];
        in.readFully(h);
        String hint = new String(h, "UTF-8");
        return new GroupHeartbeatResponse(errorCode, generation, hint.isEmpty() ? null : hint);
    }

}
