package com.pr_reviewer.queueforge.client;

import com.pr_reviewer.queueforge.protocol.*;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class ConsumerClient {
    private final String host;
    private final int port;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    public ConsumerClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        System.out.println("Consumer connected to broker at " + host + ":" + port);
    }

    public JoinGroupResponse joinGroup(String groupId, String consumerId, String topic) throws IOException {
        JoinGroupRequest req = new JoinGroupRequest(groupId, consumerId, topic);
        Frame frame = new Frame(MessageType.JOIN_GROUP, req.encode());
        frame.writeTo(out);

        Frame response = Frame.readFrom(in);
        return JoinGroupResponse.decode(response.payload());
    }

    public FetchResponse fetch(String topic, int partition, long offset) throws IOException {
        FetchRequest req = new FetchRequest(topic, partition, offset);
        Frame frame = new Frame(MessageType.FETCH, req.encode());
        frame.writeTo(out);

        Frame responseFrame = Frame.readFrom(in);
        return FetchResponse.decode(responseFrame.payload());
    }

    public void close() throws IOException {
        socket.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 4) {
            System.out.println("Usage: java ConsumerClient <bootstrapServers> <consumerId> <groupId> <topic>");
            return;
        }

        List<String> bootstrapServers = new ArrayList<>(Arrays.asList(args[0].split(",")));
        String consumerId = args[1];
        String groupId = args[2];
        String topic = args[3];

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
        for (int p : myPartitions) offsets.put(p, 0L);

        while (true) {
            for (int p : myPartitions) {
                try {
                    FetchResponse resp = dataConn.fetch(topic, p, offsets.get(p));
                    if (resp.found) {
                        System.out.println(consumerId + " read partition " + p + " offset " + offsets.get(p) +
                                ": " + new String(resp.value, "UTF-8"));
                        offsets.put(p, resp.nextOffset);
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


