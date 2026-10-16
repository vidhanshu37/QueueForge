package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class FetchResponse {
    public static final byte ERR_NONE = 0;
    public static final byte ERR_OFFSET_OUT_OF_RANGE = 1;   // offset < earliest (retention ne delete kiya) ya > latest
    public static final byte ERR_UNKNOWN_PARTITION = 2;

    public final boolean found;
    public final byte[] value;
    public final long nextOffset;
    public final byte errorCode;
    public final long earliestOffset;   // available first offset because we set limit 5k message
    public final long latestOffset; // last offset

    public FetchResponse(boolean found, byte[] value, long nextOffset,
                         byte errorCode, long earliestOffset, long latestOffset) {
        this.found = found;
        this.value = value;
        this.nextOffset = nextOffset;
        this.errorCode = errorCode;
        this.earliestOffset = earliestOffset;
        this.latestOffset = latestOffset;
    }

    public static FetchResponse record(byte[] value, long nextOffset, long earliest, long latest) {
        return new FetchResponse(true, value, nextOffset, ERR_NONE, earliest, latest);
    }

    // caught up: error nahi hai, bas abhi naya data nahi hai
    public static FetchResponse caughtUp(long earliest, long latest) {
        return new FetchResponse(false, null, latest, ERR_NONE, earliest, latest);
    }

    public static FetchResponse error(byte errorCode, long earliest, long latest) {
        return new FetchResponse(false, null, -1, errorCode, earliest, latest);
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeByte(found ? 1 : 0);
        out.writeByte(errorCode);
        out.writeLong(earliestOffset);
        out.writeLong(latestOffset);
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
        byte errorCode = in.readByte();
        long earliest = in.readLong();
        long latest = in.readLong();
        if (!found) {
            return new FetchResponse(false, null, errorCode == ERR_NONE ? latest : -1, errorCode, earliest, latest);
        }
        int valueLen = in.readInt();
        byte[] value = new byte[valueLen];
        in.readFully(value);
        long nextOffset = in.readLong();

        return new FetchResponse(true, value, nextOffset, errorCode, earliest, latest);
    }

}