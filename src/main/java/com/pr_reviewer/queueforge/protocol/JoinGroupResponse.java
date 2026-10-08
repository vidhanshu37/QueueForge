package com.pr_reviewer.queueforge.protocol;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class JoinGroupResponse {
    public final boolean success;
    public final List<Integer> assignedPartitions;
    public final String leaderHint;

    public JoinGroupResponse(boolean success, List<Integer> assignedPartitions, String leaderHint) {
        this.success = success;
        this.assignedPartitions = assignedPartitions;
        this.leaderHint = leaderHint;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeByte(success ? 1 : 0);

        if(success) {
            out.writeInt(assignedPartitions.size());
            for (int partition : assignedPartitions) {
                out.writeInt(partition);
            }
        } else {
            byte[] hintBytes = (leaderHint != null ? leaderHint : "").getBytes("UTF-8");
            out.writeInt(hintBytes.length);
            out.write(hintBytes);
        }

        return baos.toByteArray();
    }

    public static JoinGroupResponse decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

        boolean success = in.readByte() == 1;

        if (success) {
            int count = in.readInt();
            List<Integer> assignedPartitions = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                assignedPartitions.add(in.readInt());
            }
            return new JoinGroupResponse(true, assignedPartitions, null);
        } else {
            int hintLen = in.readInt();
            byte[] hintBytes = new byte[hintLen];
            in.readFully(hintBytes);
            String hint = new String(hintBytes, "UTF-8");
            return new JoinGroupResponse(false, null, hint);
        }
    }
}