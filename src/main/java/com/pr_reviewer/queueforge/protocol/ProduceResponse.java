package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class ProduceResponse {
    public final boolean success;
    public final long offset;
    public final String leaderHint;

    public ProduceResponse(boolean success, long offset, String leaderHint) {
        this.success = success;
        this.offset = offset;
        this.leaderHint = leaderHint;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeByte(success ? 1 : 0);
        if (success) {
            out.writeLong(offset);
        } else {
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
            int hintLen = in.readInt();
            byte[] hintBytes = new byte[hintLen];
            in.readFully(hintBytes);
            String hint = new String(hintBytes, "UTF-8");
            return new ProduceResponse(false, -1, hint);
        }
    }
}
