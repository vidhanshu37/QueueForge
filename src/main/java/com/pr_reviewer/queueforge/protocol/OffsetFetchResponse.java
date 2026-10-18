package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class OffsetFetchResponse {
    public static final long NO_COMMIT = -1;

    public final byte errorCode;
    public final long offset;
    public final String leaderHint;

    public OffsetFetchResponse(byte errorCode, long offset, String leaderHint) {
        this.errorCode = errorCode; this.offset = offset; this.leaderHint = leaderHint;
    }

    public boolean success() { return errorCode == OffsetCommitResponse.ERR_NONE; }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeByte(errorCode);
        out.writeLong(offset);
        byte[] h = (leaderHint == null ? "" : leaderHint).getBytes("UTF-8");
        out.writeInt(h.length); out.write(h);
        return baos.toByteArray();
    }

    public static OffsetFetchResponse decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        byte errorCode = in.readByte();
        long offset = in.readLong();
        byte[] h = new byte[in.readInt()]; in.readFully(h);
        String hint = new String(h, "UTF-8");
        return new OffsetFetchResponse(errorCode, offset, hint.isEmpty() ? null : hint);
    }
}
