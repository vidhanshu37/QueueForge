package com.pr_reviewer.queueforge.broker;

import java.util.TreeMap;

public class ConsistentHashRing {
    private final TreeMap<Long, String> ring = new TreeMap<>();
    private final int virtualNodesPerBroker = 50;

    private long fnv1aHash(String input) {
        long hash = 0xcbf29ce484222325L; // FNV offset basis
        long prime = 0x100000001b3L;     // FNV prime

        for (byte b : input.getBytes()) {
            hash ^= (b & 0xff);
            hash *= prime;
        }

        hash ^= (hash >>> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= (hash >>> 33);
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= (hash >>> 33);

        return hash;
    }

    public void addBroker(String brokerName) {
        for (int i = 0; i < virtualNodesPerBroker; i++) {
            String virtualNodeKey = brokerName + "-vnode-" + i;
            long hash = fnv1aHash(virtualNodeKey);
            ring.put(hash, brokerName);
        }
        System.out.println("Added broker '" + brokerName + "' with " + virtualNodesPerBroker + " virtual nodes");
    }

    public void removeBroker(String brokerName) {
        ring.entrySet().removeIf(entry -> entry.getValue().equals(brokerName));
        System.out.println("Removed broker '" + brokerName + "' from ring");
    }

    public String getBrokerFor(String key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("No brokers in the ring");
        }

        long hash = fnv1aHash(key);

        // move clockwise with ceilingKey() and find nearest node
        Long targetKey = ring.ceilingKey(hash);

        if (targetKey == null) {
            // circular ring
            targetKey = ring.firstKey();
        }

        return ring.get(targetKey);
    }
}
