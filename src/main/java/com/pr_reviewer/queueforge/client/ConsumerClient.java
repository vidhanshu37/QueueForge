package com.pr_reviewer.queueforge.client;

import com.pr_reviewer.queueforge.protocol.*;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        String leaderHost = args[0];
        int leaderPort = Integer.parseInt(args[1]);
        String followerHost = args[2];
        int followerPort = Integer.parseInt(args[3]);
        String consumerId = args[4];
        String groupId = args[5];
        String topic = args[6];

        ConsumerClient coordinatorConn = new ConsumerClient(leaderHost, leaderPort);
        coordinatorConn.connect();
        JoinGroupResponse joinResp = coordinatorConn.joinGroup(groupId, consumerId, topic);
        List<Integer> myPartitions = joinResp.assignedPartitions;
        System.out.println(consumerId + " assigned partitions: " + myPartitions);
        coordinatorConn.close();

        ConsumerClient dataConn = new ConsumerClient(followerHost, followerPort);
        dataConn.connect();

        Map<Integer, Long> offsets = new HashMap<>();
        for (int p : myPartitions) offsets.put(p, 0L);

        while (true) {
            for (int p : myPartitions) {
                FetchResponse resp = dataConn.fetch(topic, p, offsets.get(p));
                if (resp.found) {
                    System.out.println(consumerId + " read partition " + p + " offset " + offsets.get(p) +
                            ": " + new String(resp.value, "UTF-8"));
                    offsets.put(p, resp.nextOffset);
                }
            }
            Thread.sleep(500);
        }
    }

}
