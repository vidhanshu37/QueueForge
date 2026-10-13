package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class RequestVoteResponse {
    public final boolean voteGranted;
    public final int term;

    public RequestVoteResponse(boolean voteGranted, int term) {
        this.voteGranted = voteGranted;
        this.term = term;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeByte(voteGranted ? 1 : 0);
        out.writeInt(term);

        return baos.toByteArray();
    }

    public static RequestVoteResponse decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

        boolean voteGranted = in.readByte() == 1;
        int term = in.readInt();

        return new RequestVoteResponse(voteGranted, term);
    }
}
