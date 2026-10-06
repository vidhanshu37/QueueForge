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
        // Usage: java ConsumerClient <bootstrapServers> <consumerId> <groupId> <topic>
        List<String> bootstrapServers = new ArrayList<>(Arrays.asList(args[0].split(",")));
        String consumerId = args[1];
        String groupId = args[2];
        String topic = args[3];

        ConsumerClient coordinatorConn = connectToAny(bootstrapServers);
        JoinGroupResponse joinResp = coordinatorConn.joinGroup(groupId, consumerId, topic);
        List<Integer> myPartitions = joinResp.assignedPartitions;
        System.out.println(consumerId + " assigned partitions: " + myPartitions);

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
            }
        }
        throw new IOException("No brokers reachable", lastError);
    }
}


