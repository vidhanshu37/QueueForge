package com.pr_reviewer.queueforge.client;

import com.pr_reviewer.queueforge.protocol.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;

public class ConsumerClient {
    private final String host;
    private final int port;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int FETCH_TIMEOUT_MS = 3000;
    private static final int JOIN_TIMEOUT_MS = 5000;
    private int currentTimeoutMs = -1;

    public ConsumerClient(String host, int port) {
        this.host = host;
        this.port = port;
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

        String startPolicy = "earliest";
        for (int i = 4; i < args.length - 1; i++) {
            if (args[i].equals("--from")) startPolicy = args[i + 1];
        }
        boolean fromLatest = startPolicy.equals("latest");
        Long fromExact = null;
        if (!startPolicy.equals("earliest") && !fromLatest) {
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
        coordinatorConn.close();

        ConsumerClient dataConn = connectToAny(bootstrapServers);
        Map<Integer, Long> offsets = new HashMap<>();
        for (int p : myPartitions) {
            ListOffsetsResponse lo = dataConn.listOffsets(topic, p);
            long start;
            if (fromExact != null) start = fromExact;
            else if (fromLatest) start = lo.latestOffset;
            else start = lo.earliestOffset;
            offsets.put(p, start);
            System.out.println(consumerId + " partition " + p + " start=" + start
                    + " (policy=" + startPolicy + ", earliest=" + lo.earliestOffset + ", latest=" + lo.latestOffset + ")");
        }

        while (true) {
            for (int p : myPartitions) {
                try {
                    FetchResponse resp = dataConn.fetch(topic, p, offsets.get(p));
                    if (resp.found) {
                        System.out.println(consumerId + " read partition " + p + " offset " + offsets.get(p) +
                                ": " + new String(resp.value, "UTF-8"));
                        offsets.put(p, resp.nextOffset);
                    } else if (resp.errorCode == FetchResponse.ERR_OFFSET_OUT_OF_RANGE) {
                        long resetTo = fromLatest ? resp.latestOffset : resp.earliestOffset;
                        System.out.println("[WARN] " + consumerId + " partition " + p + " offset " + offsets.get(p)
                                + " out of range (earliest=" + resp.earliestOffset + ", latest=" + resp.latestOffset
                                + "). Resetting to " + resetTo + " (policy=" + (fromLatest ? "latest" : "earliest") + ")");
                        offsets.put(p, resetTo);
                    }
                } catch (IOException e) {
                    System.out.println("Connection lost (" + e.getMessage() + "), reconnecting...");
                    try { dataConn.close(); } catch (IOException ignored) {}
                    dataConn = connectToAny(bootstrapServers);
                }
            }
            Thread.sleep(500);
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