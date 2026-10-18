package com.pr_reviewer.queueforge.client;

import com.pr_reviewer.queueforge.protocol.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerClient {
    private final String host;
    private final int port;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int FETCH_TIMEOUT_MS = 3000;
    private static final int JOIN_TIMEOUT_MS = 5000;
    private static final long COMMIT_INTERVAL_MS = 1000;
    private int currentTimeoutMs = -1;

    public ConsumerClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String address() {
        return host + ":" + port;
    }

    public void connect() throws IOException {
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        System.out.println("Consumer connected to broker at " + host + ":" + port);
    }

    public JoinGroupResponse joinGroup(String groupId, String consumerId, String topic) throws IOException {
        setReadTimeout(JOIN_TIMEOUT_MS);
        JoinGroupRequest req = new JoinGroupRequest(groupId, consumerId, topic);
        Frame frame = new Frame(MessageType.JOIN_GROUP, req.encode());
        frame.writeTo(out);

        Frame response = Frame.readFrom(in);
        return JoinGroupResponse.decode(response.payload());
    }

    public FetchResponse fetch(String topic, int partition, long offset) throws IOException {
        setReadTimeout(FETCH_TIMEOUT_MS);
        FetchRequest req = new FetchRequest(topic, partition, offset);
        Frame frame = new Frame(MessageType.FETCH, req.encode());
        frame.writeTo(out);

        Frame responseFrame = Frame.readFrom(in);
        return FetchResponse.decode(responseFrame.payload());
    }

    public ListOffsetsResponse listOffsets(String topic, int partition) throws IOException {
        setReadTimeout(FETCH_TIMEOUT_MS);
        ListOffsetsRequest req = new ListOffsetsRequest(topic, partition);
        new Frame(MessageType.LIST_OFFSETS, req.encode()).writeTo(out);

        Frame responseFrame = Frame.readFrom(in);
        return ListOffsetsResponse.decode(responseFrame.payload());
    }

    public OffsetCommitResponse commitOffset(String groupId, String topic, int partition, long offset) throws IOException {
        setReadTimeout(FETCH_TIMEOUT_MS * 3);
        OffsetCommitRequest req = new OffsetCommitRequest(groupId, topic, partition, offset);
        new Frame(MessageType.OFFSET_COMMIT, req.encode()).writeTo(out);
        return OffsetCommitResponse.decode(Frame.readFrom(in).payload());
    }

    public OffsetFetchResponse fetchCommitted(String groupId, String topic, int partition) throws IOException {
        setReadTimeout(FETCH_TIMEOUT_MS);
        OffsetFetchRequest req = new OffsetFetchRequest(groupId, topic, partition);
        new Frame(MessageType.OFFSET_FETCH, req.encode()).writeTo(out);
        return OffsetFetchResponse.decode(Frame.readFrom(in).payload());
    }

    private void setReadTimeout(int ms) throws SocketException {
        if (ms != currentTimeoutMs) {
            socket.setSoTimeout(ms);
            currentTimeoutMs = ms;
        }
    }

    public void close() throws IOException {
        socket.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 4) {
            System.out.println("Usage: java ConsumerClient <bootstrapServers> <consumerId> <groupId> <topic> [--from earliest|latest|<offset>]");
            return;
        }

        List<String> bootstrapServers = new ArrayList<>(Arrays.asList(args[0].split(",")));
        String consumerId = args[1];
        String groupId = args[2];
        String topic = args[3];

        String startPolicy = null;
        for (int i = 4; i < args.length - 1; i++) {
            if (args[i].equals("--from")) startPolicy = args[i + 1];
        }
        boolean explicitFrom = startPolicy != null;
        boolean fromLatest = "latest".equals(startPolicy);
        Long fromExact = null;
        if (explicitFrom && !fromLatest && !startPolicy.equals("earliest")) {
            fromExact = Long.parseLong(startPolicy);
        }

        ConsumerClient coordinatorConn = connectToAny(bootstrapServers);
        JoinGroupResponse joinResp = null;
        int attempts = 0;

        while (joinResp == null || !joinResp.success) {
            attempts++;
            if (attempts > 5) {
                System.out.println("Could not join group after 5 attempts, exiting.");
                return;
            }

            joinResp = coordinatorConn.joinGroup(groupId, consumerId, topic);

            if (!joinResp.success) {
                if (joinResp.leaderHint != null && !joinResp.leaderHint.isEmpty()) {
                    System.out.println("NOT_LEADER! Redirecting JOIN_GROUP to: " + joinResp.leaderHint);
                    coordinatorConn.close();
                    String[] parts = joinResp.leaderHint.split(":");
                    coordinatorConn = new ConsumerClient(parts[0], Integer.parseInt(parts[1]));
                    coordinatorConn.connect();
                } else {
                    System.out.println("No leader hint (election in progress?), retrying via bootstrap list...");
                    Thread.sleep(1000);
                    coordinatorConn.close();
                    coordinatorConn = connectToAny(bootstrapServers);
                }
            }
        }

        List<Integer> myPartitions = joinResp.assignedPartitions;
        System.out.println(consumerId + " assigned partitions: " + myPartitions);
        LeaderLink leader = new LeaderLink(bootstrapServers, coordinatorConn);

        ConsumerClient dataConn = connectToAny(bootstrapServers);
        Map<Integer, Long> offsets = new ConcurrentHashMap<>();
        Map<Integer, Long> committed = new ConcurrentHashMap<>();
        for (int p : myPartitions) {
            ListOffsetsResponse lo = dataConn.listOffsets(topic, p);
            long start;
            String source;
            if (explicitFrom) {
                start = fromExact != null ? fromExact : (fromLatest ? lo.latestOffset : lo.earliestOffset);
                source = "--from " + startPolicy;
            } else {
                OffsetFetchResponse c = leader.fetchCommitted(groupId, topic, p, 6);
                if (c == null) {
                    System.out.println("[ERROR] could not read committed offset for partition " + p
                            + " (leader unreachable?). Not guessing a start position, exiting.");
                    return;
                }
                if (c.offset != OffsetFetchResponse.NO_COMMIT) {
                    start = c.offset;
                    committed.put(p, c.offset);
                    source = "committed";
                } else {
                    start = lo.earliestOffset;
                    source = "no commit yet, earliest";
                }
            }
            offsets.put(p, start);
            System.out.println(consumerId + " partition " + p + " start=" + start + " (" + source
                    + ", earliest=" + lo.earliestOffset + ", latest=" + lo.latestOffset + ")");
        }

        Runnable commitDirty = () -> {
            for (int p : myPartitions) {
                Long cur = offsets.get(p);
                if (cur != null && !cur.equals(committed.get(p))) {
                    if (leader.commit(groupId, topic, p, cur, 2)) {
                        committed.put(p, cur);
                        System.out.println(consumerId + " committed partition " + p + " = " + cur);
                    } else {
                        System.out.println("[WARN] " + consumerId + " commit failed for partition " + p + ", will retry");
                    }
                }
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(commitDirty, "commit-on-exit"));
        long lastCommitMs = System.currentTimeMillis();

        while (true) {
            boolean gotAny = false;
            for (int p : myPartitions) {
                try {
                    FetchResponse resp = dataConn.fetch(topic, p, offsets.get(p));
                    if (resp.found) {
                        gotAny = true;
                        System.out.println(consumerId + " read partition " + p + " offset " + offsets.get(p) +
                                ": " + new String(resp.value, "UTF-8"));
                        offsets.put(p, resp.nextOffset);
                    } else if (resp.errorCode == FetchResponse.ERR_OFFSET_OUT_OF_RANGE) {
                        long cur = offsets.get(p);
                        long earliest = resp.earliestOffset;
                        long latest = resp.latestOffset;
                        if (cur > resp.latestOffset) {
                            ListOffsetsResponse lo = leader.listOffsets(groupId, topic, p);
                            if (lo == null) {
                                System.out.println("[WARN] " + consumerId + " partition " + p + ": offset " + cur
                                        + " is beyond this broker's log end " + resp.latestOffset
                                        + ", could not verify with leader, will retry (not resetting)");
                                continue;
                            }
                            if (cur <= lo.latestOffset) {
                                System.out.println("[WARN] " + consumerId + " partition " + p + ": broker " + dataConn.address()
                                        + " is behind (latest=" + resp.latestOffset + "), leader has up to " + lo.latestOffset
                                        + ". Switching data connection to leader");
                                String leaderAddr = leader.address();
                                if (leaderAddr != null && !leaderAddr.equals(dataConn.address())) {
                                    try { dataConn.close(); } catch (IOException ignored) {}
                                    String[] hp = leaderAddr.split(":");
                                    dataConn = new ConsumerClient(hp[0], Integer.parseInt(hp[1]));
                                    dataConn.connect();
                                }
                                continue;
                            }
                            earliest = lo.earliestOffset;
                            latest = lo.latestOffset;
                        }
                        long resetTo = fromLatest ? latest : earliest;
                        System.out.println("[WARN] " + consumerId + " partition " + p + " offset " + cur
                                + " out of range (earliest=" + earliest + ", latest=" + latest
                                + "). Resetting to " + resetTo + " (policy=" + (fromLatest ? "latest" : "earliest") + ")");
                        offsets.put(p, resetTo);
                    }
                } catch (IOException e) {
                    System.out.println("Connection lost (" + e.getMessage() + "), reconnecting...");
                    try { dataConn.close(); } catch (IOException ignored) {}
                    dataConn = connectToAny(bootstrapServers);
                }
            }
            if (System.currentTimeMillis() - lastCommitMs >= COMMIT_INTERVAL_MS) {
                commitDirty.run();
                lastCommitMs = System.currentTimeMillis();
            }
            if (!gotAny) {
                Thread.sleep(500);
            }
        }
    }

    private static class LeaderLink {
        private final List<String> bootstrap;
        private ConsumerClient conn;
        private String confirmedLeader;

        LeaderLink(List<String> bootstrap, ConsumerClient leaderConn) {
            this.bootstrap = bootstrap;
            this.conn = leaderConn;
            this.confirmedLeader = leaderConn.address();
        }

        synchronized String address() {
            return confirmedLeader;
        }

        synchronized OffsetFetchResponse fetchCommitted(String group, String topic, int p, int maxAttempts) {
            String hint = null;
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    if (conn == null) conn = open(hint);
                    OffsetFetchResponse r = conn.fetchCommitted(group, topic, p);
                    if (r.success()) {
                        confirmedLeader = conn.address();
                        return r;
                    }
                    if (r.errorCode == OffsetCommitResponse.ERR_NOT_LEADER) {
                        hint = r.leaderHint;
                        drop();
                    }
                } catch (IOException e) {
                    hint = null;
                    drop();
                }
                pause(attempt);
            }
            return null;
        }

        synchronized boolean commit(String group, String topic, int p, long offset, int maxAttempts) {
            String hint = null;
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    if (conn == null) conn = open(hint);
                    OffsetCommitResponse r = conn.commitOffset(group, topic, p, offset);
                    if (r.success()) {
                        confirmedLeader = conn.address();
                        return true;
                    }
                    if (r.errorCode == OffsetCommitResponse.ERR_NOT_LEADER) {
                        hint = r.leaderHint;
                        drop();
                    }
                } catch (IOException e) {
                    hint = null;
                    drop();
                }
                pause(attempt);
            }
            return false;
        }

        synchronized ListOffsetsResponse listOffsets(String group, String topic, int p) {
            if (confirmedLeader == null || conn == null) {
                if (fetchCommitted(group, topic, p, 3) == null) return null;
            }
            try {
                return conn.listOffsets(topic, p);
            } catch (IOException e) {
                drop();
                return null;
            }
        }

        private ConsumerClient open(String hint) throws IOException {
            if (hint != null && !hint.isEmpty()) {
                String[] hp = hint.split(":");
                ConsumerClient c = new ConsumerClient(hp[0], Integer.parseInt(hp[1]));
                c.connect();
                return c;
            }
            return connectToAny(bootstrap);
        }

        private void drop() {
            if (conn != null) {
                try { conn.close(); } catch (IOException ignored) {}
            }
            conn = null;
            confirmedLeader = null;
        }

        private void pause(int attempt) {
            try {
                Thread.sleep(300L * attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static ConsumerClient connectToAny(List<String> bootstrapServers) throws IOException {
        IOException lastError = null;
        for (String server : bootstrapServers) {
            try {
                String[] parts = server.split(":");
                ConsumerClient client = new ConsumerClient(parts[0], Integer.parseInt(parts[1]));
                client.connect();
                return client;
            } catch (IOException e) {
                lastError = e;
                System.out.println("Could not connect to " + server + ", trying next...");
            }
        }
        throw new IOException("No brokers reachable from bootstrap list", lastError);
    }
}