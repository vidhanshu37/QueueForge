package com.pr_reviewer.queueforge.protocol;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class JoinGroupResponse {
    public final List<Integer> assignedPartitions;

    public JoinGroupResponse(List<Integer> assignedPartitions) {
        this.assignedPartitions = assignedPartitions;
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeInt(assignedPartitions.size());
        for (int partition : assignedPartitions) {
            out.writeInt(partition);
        }

        return baos.toByteArray();
    }

    public static JoinGroupResponse decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));

        int count = in.readInt();
        List<Integer> assignedPartitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            assignedPartitions.add(in.readInt());
        }

        return new JoinGroupResponse(assignedPartitions);
    }
}