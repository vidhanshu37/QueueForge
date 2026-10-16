package com.pr_reviewer.queueforge.protocol;

import java.util.Arrays;

public final class Partitioner {
    private Partitioner() {}

    public static int partitionFor(byte[] key, int numPartitions) {
        return Math.floorMod(Arrays.hashCode(key), numPartitions);
    }
}

