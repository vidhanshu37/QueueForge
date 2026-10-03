package com.pr_reviewer.queueforge.broker;

import com.pr_reviewer.queueforge.protocol.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerServer {
    private final Map<String, List<Partition>> topicPartitions = new ConcurrentHashMap<>();
    private final int numPartitionPerTopic = 3;

    private final int port;

    public BrokerServer(int port) {
        this.port = port;
    }

    private List<Partition> getOrCreatePartitions(String topic) {
        return topicPartitions.computeIfAbsent(topic, t -> {
            List<Partition> partitions = new ArrayList<>();
            new File("data").mkdirs();

            for (int i = 0; i < numPartitionPerTopic; i++) {
                try {
                    String logPath = "data/" + topic + "-" + i + ".log";
                    partitions.add(new Partition(logPath));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create partition log file for " + topic + "-" + i, e);
                }
            }
            return partitions;
        });
    }

    private int selectPartition(String topic, int requestedPartition, byte[] key) {
        List<Partition> partitions = getOrCreatePartitions(topic);
        if (requestedPartition >= 0) {
            return requestedPartition;
        }
        if (key != null) {
            int hash = java.util.Arrays.hashCode(key);
            return Math.floorMod(hash, partitions.size());
        }
        return new Random().nextInt(partitions.size());
    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Broker server started on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("Accepted connection from " + clientSocket.getRemoteSocketAddress());
            Thread handlerThread = new Thread(() -> handleClient(clientSocket));
            handlerThread.start();
        }
    }

    private void handleClient(Socket socket) {
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            while (true) {
                Frame frame = Frame.readFrom(in);

                if (frame.type() == MessageType.PRODUCE) {
                    ProduceRequest req = ProduceRequest.decode(frame.payload());
                    handleProduce(req);

                    byte[] ackPayload = req.topic.getBytes("UTF-8");
                    Frame response = new Frame(MessageType.PRODUCE_RESPONSE, ackPayload);
                    response.writeTo(out);
                } else if (frame.type() == MessageType.FETCH) {
                    FetchRequest req = FetchRequest.decode(frame.payload());
                    FetchResponse resp = handleFetch(req);
                    Frame response = new Frame(MessageType.FETCH_RESPONSE, resp.encode());
                    response.writeTo(out);
                }
                else {
                    System.out.println("Received unknown message type: " + frame.type());
                }
            }
        } catch (EOFException e) {
            System.out.println("Client disconnected: " + socket.getRemoteSocketAddress());
        } catch (IOException e) {
            System.out.println("Connection error: " + e.getMessage());
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private FetchResponse handleFetch(FetchRequest req) throws IOException {
        List<Partition> partitions = getOrCreatePartitions(req.topic);
        if (req.partition < 0 || req.partition >= partitions.size()) {
            return new FetchResponse(false, null, -1);
        }
        Partition partition = partitions.get(req.partition);
        byte[] value = partition.read(req.offset);
        if (value == null) {
            return new FetchResponse(false, null, -1); // nothing at this offset yet
        }
        return new FetchResponse(true, value, req.offset + 1);
    }

    private void handleProduce(ProduceRequest req) throws IOException {
        List<Partition> partitions = getOrCreatePartitions(req.topic);
        int targetPartition = selectPartition(req.topic, req.partition, req.key);
        long offset = partitions.get(targetPartition).append(req.value);

        System.out.println("Stored message in topic '" + req.topic + "' partition " + targetPartition +
                " at offset " + offset + ": " + new String(req.value, java.nio.charset.StandardCharsets.UTF_8));
    }

    public static void main(String[] args) throws IOException {
        int port = 9092;
        new BrokerServer(port).start();
    }
}