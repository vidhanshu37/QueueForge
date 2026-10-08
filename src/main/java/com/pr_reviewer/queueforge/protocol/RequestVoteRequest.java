package com.pr_reviewer.queueforge.protocol;

import java.io.*;

public class RequestVoteRequest {
    public final int term;
    public final String candidateId;

    public RequestVoteRequest(int term, String candidateId) {
        this.term = term;
        this.candidateId = candidateId;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeInt(term);
        byte[] candidateIdBytes = candidateId.getBytes("UTF-8");
        out.writeInt(candidateIdBytes.length);
        out.write(candidateIdBytes);

        return baos.toByteArray();
    }

    public static RequestVoteRequest decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

        int term = in.readInt();
        int idLen = in.readInt();
        byte[] idBytes = new byte[idLen];
        in.readFully(idBytes);
        String candidateId = new String(idBytes, "UTF-8");

        return new RequestVoteRequest(term, candidateId);
    }
}
