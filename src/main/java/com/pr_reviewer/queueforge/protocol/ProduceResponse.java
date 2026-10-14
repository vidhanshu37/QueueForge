package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class ProduceResponse {
    public static final byte ERR_NONE = 0;
    public static final byte ERR_NOT_LEADER = 1;
    public static final byte ERR_NOT_ENOUGH_REPLICAS = 2;               // append se pehle reject
    public static final byte ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND = 3; // leader par likha ja chuka hai

    public final boolean success;
    public final long offset;
    public final byte errorCode;
    public final String leaderHint;

    public ProduceResponse(boolean success, long offset, String leaderHint) {
        this(success, offset, success ? ERR_NONE : ERR_NOT_LEADER, leaderHint);
    }


    public ProduceResponse(boolean success, long offset, byte errorCode, String leaderHint) {
        this.success = success;
        this.offset = offset;
        this.errorCode = errorCode;
        this.leaderHint = leaderHint;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeByte(success ? 1 : 0);
        if (success) {
            out.writeLong(offset);
        } else {
            out.writeByte(errorCode);
            byte[] hintBytes = (leaderHint != null ? leaderHint : "").getBytes("UTF-8");
            out.writeInt(hintBytes.length);
            out.write(hintBytes);
        }
        return baos.toByteArray();
    }

    public static ProduceResponse decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

        boolean success = in.readByte() == 1;
        if (success) {
            long offset = in.readLong();
            return new ProduceResponse(true, offset, null);
        } else {
            byte errorCode = in.readByte();
            int hintLen = in.readInt();
            byte[] hintBytes = new byte[hintLen];
            in.readFully(hintBytes);
            return new ProduceResponse(false, -1, errorCode, new String(hintBytes, "UTF-8"));
        }
    }
}
