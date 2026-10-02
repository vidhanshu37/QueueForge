package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class FetchResponse {
    public final boolean found;
    public final byte[] value;
    public final long nextOffset;

    public FetchResponse(boolean found, byte[] value, long nextOffset) {
        this.found = found;
        this.value = value;
        this.nextOffset = nextOffset;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeByte(found ? 1 : 0);
        if (found) {
            out.writeInt(value.length);
            out.write(value);
            out.writeLong(nextOffset);
        }

        return baos.toByteArray();
    }

    public static FetchResponse decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

        boolean found = in.readByte() == 1;
        if (!found) {
            return new FetchResponse(false, null, -1);
        }
        int valueLen = in.readInt();
        byte[] value = new byte[valueLen];
        in.readFully(value);
        long nextOffset = in.readLong();

        return new FetchResponse(true, value, nextOffset);
    }

}
