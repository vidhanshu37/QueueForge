package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class ListOffsetsResponse {
    public static final byte ERR_NONE = 0;
    public static final byte ERR_UNKNOWN_PARTITION = 2;

    public final byte errorCode;
    public final int numPartitions;
    public final long earliestOffset;
    public final long latestOffset;

    public ListOffsetsResponse(byte errorCode, int numPartitions, long earliestOffset, long latestOffset) {
        this.errorCode = errorCode;
        this.numPartitions = numPartitions;
        this.earliestOffset = earliestOffset;
        this.latestOffset = latestOffset;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeByte(errorCode);
        out.writeInt(numPartitions);
        out.writeLong(earliestOffset);
        out.writeLong(latestOffset);
        return baos.toByteArray();
    }

    public static ListOffsetsResponse decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        byte errorCode = in.readByte();
        int numPartitions = in.readInt();
        long earliest = in.readLong();
        long latest = in.readLong();
        return new ListOffsetsResponse(errorCode, numPartitions, earliest, latest);
    }
}