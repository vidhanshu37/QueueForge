package com.pr_reviewer.queueforge.broker;

import com.pr_reviewer.queueforge.protocol.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class BrokerServer {

    private static class ReplicationTask {
        String topic;
        int partition;
        long offset;
        byte[] value;

        ReplicationTask(String topic, int partition, long offset, byte[] value) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.value = value;
        }
    }

    private final Map<String, BlockingQueue<ReplicationTask>> replicationQueues = new ConcurrentHashMap<>();

    private final Map<String, LinkedHashMap<String, Long>> consumerGroups = new ConcurrentHashMap<>();

    private final Map<String, Map<String, Long>> followerOffsets = new ConcurrentHashMap<>();
    private final int maxLag = 5;

    public enum Role { LEADER, FOLLOWER }

    private final Role role;
    private final List<String> followerAddresses;

    private final Map<String, Socket> followerSockets = new HashMap<>();
    private final Map<String, DataOutputStream> followerOutputs = new HashMap<>();
    private final Map<String, DataInputStream> followerInputs = new HashMap<>();

    private final Map<String, List<Partition>> topicPartitions = new ConcurrentHashMap<>();
    private final int numPartitionPerTopic = 3;

    private final int port;

    public BrokerServer(int port, Role role, List<String> followerAddresses) {
        this.port = port;
        this.role = role;
        this.followerAddresses = followerAddresses;
    }

    private void connectToFollowers() throws IOException {
        for (String address : followerAddresses) {
            String[] parts = address.split(":");
            Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
            followerSockets.put(address, socket);
            followerOutputs.put(address, new DataOutputStream(new BufferedOutputStream(socket.getOutputStream())));
            followerInputs.put(address, new DataInputStream(new BufferedInputStream(socket.getInputStream())));
            System.out.println("Connected to follower at " + address);

            BlockingQueue<ReplicationTask> queue = new LinkedBlockingQueue<>();
            replicationQueues.put(address, queue);

            Thread senderThread = new Thread(() -> runReplicationSender(address, queue));
            senderThread.setDaemon(true);
            senderThread.start();
        }
    }

    private void runReplicationSender(String address, BlockingQueue<ReplicationTask> queue) {
        DataOutputStream out = followerOutputs.get(address);
        DataInputStream in = followerInputs.get(address);

        while (true) {
            try {
                ReplicationTask task = queue.take(); // blocks jab tak koi task na aaye, FIFO order mein

                ReplicateRequest repReq = new ReplicateRequest(task.topic, task.partition, task.offset, task.value);
                Frame repFrame = new Frame(MessageType.REPLICATE, repReq.encode());

                repFrame.writeTo(out);
                Frame response = Frame.readFrom(in);

                String key = task.topic + "-" + task.partition;
                followerOffsets.computeIfAbsent(key, k -> new ConcurrentHashMap<>()).put(address, task.offset);

            } catch (InterruptedException e) {
                break;
            } catch (IOException e) {
                System.out.println("Replication sender for " + address + " failed: " + e.getMessage());
            }
        }
    }

    private List<Partition> getOrCreatePartitions(String topic) {
        return topicPartitions.computeIfAbsent(topic, t -> {
            List<Partition> partitions = new ArrayList<>();

            String dataDir = "data/" + "data-" + port;
            new File(dataDir).mkdirs();

            for (int i = 0; i < numPartitionPerTopic; i++) {
                try {
                    String partitionDir = dataDir + "/" + topic + "-" + i;
                    partitions.add(new Partition(partitionDir));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create partition dir for " + topic + "-" + i, e);
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
        if(role == Role.LEADER) {
            connectToFollowers();
        }

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
                } else if (frame.type() == MessageType.JOIN_GROUP) {
                    JoinGroupRequest req = JoinGroupRequest.decode(frame.payload());
                    JoinGroupResponse resp = handleJoinGroup(req);
                    Frame response = new Frame(MessageType.JOIN_GROUP_RESPONSE, resp.encode());
                    response.writeTo(out);
                }
                else if (frame.type() == MessageType.FETCH) {
                    FetchRequest req = FetchRequest.decode(frame.payload());
                    FetchResponse resp = handleFetch(req);
                    Frame response = new Frame(MessageType.FETCH_RESPONSE, resp.encode());
                    response.writeTo(out);
                } else if (frame.type() == MessageType.REPLICATE) {
                    ReplicateRequest repReq = ReplicateRequest.decode(frame.payload());
                    List<Partition> partitions = getOrCreatePartitions(repReq.topic);
                    partitions.get(repReq.partition).appendAt(repReq.offset, repReq.value);

                    System.out.println("[FOLLOWER] Replicated message at partition " + repReq.partition +
                            " offset " + repReq.offset);

                    Frame response = new Frame(MessageType.REPLICATE_RESPONSE, new byte[]{1});
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

        if (role == Role.LEADER) {
            if (req.ack == 0 || req.ack == 1) {
                replicateAsync(req.topic, targetPartition, offset, req.value); // bas queue mein daalo, turant return
            } else { // ack == -1
                try {
                    replicateSync(req.topic, targetPartition, offset, req.value);
                } catch (InterruptedException e) {
                    throw new IOException("Replication wait interrupted", e);
                }
                List<String> isr = getInSyncReplicas(req.topic, targetPartition, offset);
                System.out.println("ack=all: confirmed ISR members: " + isr);
            }
        }
    }

    private void replicateAsync(String topic, int partition, long offset, byte[] value) {
        ReplicationTask task = new ReplicationTask(topic, partition, offset, value);
        for (String address : followerAddresses) {
            replicationQueues.get(address).offer(task);
        }
    }

    private void replicateSync(String topic, int partition, long offset, byte[] value) throws IOException, InterruptedException {
        ReplicationTask task = new ReplicationTask(topic, partition, offset, value);
        for (String address : followerAddresses) {
            replicationQueues.get(address).offer(task);
        }
        for (String address : followerAddresses) {
            String key = topic + "-" + partition;
            while (followerOffsets.getOrDefault(key, Collections.emptyMap()).getOrDefault(address, -1L) < offset) {
                Thread.sleep(5);
            }
        }
    }

    private final Object replicationLock = new Object();
    private void replicateToFollowers(String topic, int partition, long offset, byte[] value) throws IOException {
        synchronized (replicationLock) {
            ReplicateRequest repReq = new ReplicateRequest(topic, partition, offset, value);
            Frame repFrame = new Frame(MessageType.REPLICATE, repReq.encode());

            String key = topic + "-" + partition;

            for (String address : followerAddresses) {
                DataOutputStream out = followerOutputs.get(address);
                DataInputStream in = followerInputs.get(address);

                repFrame.writeTo(out);
                Frame response = Frame.readFrom(in);

                followerOffsets.computeIfAbsent(key, k -> new ConcurrentHashMap<>()).put(address, offset);
            }
        }
    }

    private List<String> getInSyncReplicas(String topic, int partition, long leaderOffset) {
        String key = topic + "-" + partition;
        Map<String, Long> offsets = followerOffsets.getOrDefault(key, Collections.emptyMap());

        List<String> inSync = new ArrayList<>();
        for (String address : followerAddresses) {
            long followerOffset = offsets.getOrDefault(address, -1L);
            if (leaderOffset - followerOffset <= maxLag) {
                inSync.add(address);
            }
        }
        return inSync;
    }

    private synchronized JoinGroupResponse handleJoinGroup(JoinGroupRequest req) {
        LinkedHashMap<String, Long> members = consumerGroups.computeIfAbsent(req.groupId, g -> new LinkedHashMap<>());
        members.put(req.consumerId, System.currentTimeMillis());

        List<String> memberList = new ArrayList<>(members.keySet());
        int myIndex = memberList.indexOf(req.consumerId);
        int totalMembers = memberList.size();

        List<Partition> partitions = getOrCreatePartitions(req.topic);
        List<Integer> assigned = new ArrayList<>();
        for (int p = 0; p < partitions.size(); p++) {
            if (p % totalMembers == myIndex) {
                assigned.add(p);
            }
        }

        System.out.println("Group '" + req.groupId + "' now has " + totalMembers + " members. " +
                req.consumerId + " assigned partitions: " + assigned);

        return new JoinGroupResponse(assigned);
    }

    public static void main(String[] args) throws IOException {

        int port = Integer.parseInt(args[0]);
        Role role = Role.valueOf(args[1].toUpperCase());

        List<String> followerAddresses = new ArrayList<>();
        if (role == Role.LEADER && args.length > 2) {
            followerAddresses = Arrays.asList(args[2].split(","));
        }

        BrokerServer broker = new BrokerServer(port, role, followerAddresses);
        broker.start();
    }
}