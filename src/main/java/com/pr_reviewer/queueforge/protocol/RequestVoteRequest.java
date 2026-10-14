package com.pr_reviewer.queueforge.protocol;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class RequestVoteRequest {
    public final int term;
    public final String candidateId;
    public final Map<String, Long> logEnds;

    public RequestVoteRequest(int term, String candidateId, Map<String, Long> logEnds) {
        this.term = term;
        this.candidateId = candidateId;
        this.logEnds = logEnds;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeInt(term);
        byte[] candidateIdBytes = candidateId.getBytes("UTF-8");
        out.writeInt(candidateIdBytes.length);
        out.write(candidateIdBytes);

        out.writeInt(logEnds.size());
        for (Map.Entry<String, Long> e : logEnds.entrySet()) {
            byte[] keyBytes = e.getKey().getBytes("UTF-8");
            out.writeInt(keyBytes.length);
            out.write(keyBytes);
            out.writeLong(e.getValue());
        }

        return baos.toByteArray();
    }

    public static RequestVoteRequest decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

        int term = in.readInt();
        int idLen = in.readInt();
        byte[] idBytes = new byte[idLen];
        in.readFully(idBytes);
        String candidateId = new String(idBytes, "UTF-8");

        int count = in.readInt();
        Map<String, Long> logEnds = new HashMap<>();
        for (int i = 0; i < count; i++) {
            int keyLen = in.readInt();
            byte[] keyBytes = new byte[keyLen];
            in.readFully(keyBytes);
            logEnds.put(new String(keyBytes, "UTF-8"), in.readLong());
        }

        return new RequestVoteRequest(term, candidateId, logEnds);
    }
}
