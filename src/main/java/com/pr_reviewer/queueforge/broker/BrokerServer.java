package com.pr_reviewer.queueforge.broker;

import com.pr_reviewer.queueforge.protocol.*;
import com.pr_reviewer.queueforge.raft.ElectionTimer;
import com.pr_reviewer.queueforge.raft.RaftState;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

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

    private ScheduledExecutorService heartbeatScheduler;

    private final List<String> allPeers; // all nodes(peer) address expect current
    private final Map<String, Socket> peerSockets = new ConcurrentHashMap<>();
    private final Map<String, DataOutputStream> peerOutputs = new ConcurrentHashMap<>();
    private final Map<String, DataInputStream> peerInputs = new ConcurrentHashMap<>();

    private RaftState raftState;
    private ElectionTimer electionTimer;

    private final Map<String, BlockingQueue<ReplicationTask>> replicationQueues = new ConcurrentHashMap<>();

    private final Map<String, LinkedHashMap<String, Long>> consumerGroups = new ConcurrentHashMap<>();

    private final Map<String, Map<String, Long>> followerOffsets = new ConcurrentHashMap<>();
    private final int maxLag = 5;

//    public enum Role { LEADER, FOLLOWER }

//    private final Role role;
//    private final List<String> followerAddresses;

//    private final Map<String, Socket> followerSockets = new HashMap<>();
//    private final Map<String, DataOutputStream> followerOutputs = new HashMap<>();
//    private final Map<String, DataInputStream> followerInputs = new HashMap<>();

    private final Map<String, List<Partition>> topicPartitions = new ConcurrentHashMap<>();
    private final int numPartitionPerTopic = 3;

    private final int port;

    public BrokerServer(int port, List<String> allPeers) {
        this.port = port;
        this.allPeers = allPeers;
    }

    private List<String> getCurrentFollowers() {
        return allPeers;
    }

    private DataOutputStream getOrConnectPeer(String peerAddress) throws IOException {
        if (peerOutputs.containsKey(peerAddress)) {
            return peerOutputs.get(peerAddress);
        }
        String[] parts = peerAddress.split(":");
        Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
        peerSockets.put(peerAddress, socket);
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        peerOutputs.put(peerAddress, out);
        peerInputs.put(peerAddress, in);
        return out;
    }

//    private void connectToFollowers() throws IOException {
//        for (String address : followerAddresses) {
//            String[] parts = address.split(":");
//            Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
//            followerSockets.put(address, socket);
//            followerOutputs.put(address, new DataOutputStream(new BufferedOutputStream(socket.getOutputStream())));
//            followerInputs.put(address, new DataInputStream(new BufferedInputStream(socket.getInputStream())));
//            System.out.println("Connected to follower at " + address);
//
//            BlockingQueue<ReplicationTask> queue = new LinkedBlockingQueue<>();
//            replicationQueues.put(address, queue);
//
//            Thread senderThread = new Thread(() -> runReplicationSender(address, queue));
//            senderThread.setDaemon(true);
//            senderThread.start();
//        }
//    }

    private void runReplicationSender(String address, BlockingQueue<ReplicationTask> queue) {
        while (true) {
            try {
                ReplicationTask task = queue.take();
                DataOutputStream out = getOrConnectPeer(address);
                DataInputStream in = peerInputs.get(address);

                ReplicateRequest repReq = new ReplicateRequest(task.topic, task.partition, task.offset, task.value);
                Frame repFrame = new Frame(MessageType.REPLICATE, repReq.encode());
                repFrame.writeTo(out);
                Frame.readFrom(in);

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
        raftState = new RaftState("localhost:" + port);
        electionTimer = new ElectionTimer(3000, 5000, this::onElectionTimeout);
        electionTimer.reset();

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
                    ProduceResponse resp = handleProduce(req);
                    Frame response = new Frame(MessageType.PRODUCE_RESPONSE, resp.encode());
                    response.writeTo(out);
                } else if (frame.type() == MessageType.JOIN_GROUP) {
                    JoinGroupRequest req = JoinGroupRequest.decode(frame.payload());
                    JoinGroupResponse resp = handleJoinGroup(req);
                    Frame response = new Frame(MessageType.JOIN_GROUP_RESPONSE, resp.encode());
                    response.writeTo(out);
                } else if (frame.type() == MessageType.FETCH) {
                    FetchRequest req = FetchRequest.decode(frame.payload());
                    FetchResponse resp = handleFetch(req);
                    Frame response = new Frame(MessageType.FETCH_RESPONSE, resp.encode());
                    response.writeTo(out);
                } else if (frame.type() == MessageType.HEARTBEAT) {
                    HeartbeatRequest req = HeartbeatRequest.decode(frame.payload());
                    if (req.term >= raftState.getCurrentTerm()) {
                        raftState.becomeFollower(req.term);
                        raftState.setCurrentLeaderId(req.leaderId);
                        electionTimer.reset();
                    }
                    Frame response = new Frame(MessageType.HEARTBEAT_RESPONSE, new byte[]{1});
                    response.writeTo(out);
                } else if (frame.type() == MessageType.REPLICATE) {
                    ReplicateRequest repReq = ReplicateRequest.decode(frame.payload());
                    List<Partition> partitions = getOrCreatePartitions(repReq.topic);

                    try {
                        partitions.get(repReq.partition).appendAt(repReq.offset, repReq.value);
                        System.out.println("[FOLLOWER] Replicated message at partition " + repReq.partition +
                                " offset " + repReq.offset);
                    } catch (IllegalStateException ex) {
                        System.out.println("[FOLLOWER] Offset miss-match detected (expected sync issue after leader change): " + ex.getMessage());
                    }

                    Frame response = new Frame(MessageType.REPLICATE_RESPONSE, new byte[]{1});
                    response.writeTo(out);
                } else if (frame.type() == MessageType.REQUEST_VOTE) {
                    RequestVoteRequest req = RequestVoteRequest.decode(frame.payload());
                    boolean granted = raftState.handleVoteRequest(req.term, req.candidateId);
                    RequestVoteResponse resp = new RequestVoteResponse(granted, raftState.getCurrentTerm());
                    Frame response = new Frame(MessageType.REQUEST_VOTE_RESPONSE, resp.encode());
                    response.writeTo(out);
                } else {
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
            return new FetchResponse(false, null, -1);
        }
        return new FetchResponse(true, value, req.offset + 1);
    }

    private ProduceResponse handleProduce(ProduceRequest req) throws IOException {
        if(raftState.getRole() != RaftState.NodeRole.LEADER) {
            String hint = raftState.getCurrentLeaderId();
            System.out.println("NOT_LEADER - redirecting producer to: " + hint);
            return new ProduceResponse(false, -1, hint);
        }

        List<Partition> partitions = getOrCreatePartitions(req.topic);
        int targetPartition = selectPartition(req.topic, req.partition, req.key);
        long offset = partitions.get(targetPartition).append(req.value);

        System.out.println("Stored message in topic '" + req.topic + "' partition " + targetPartition + " at offset " + offset + ": " + new String(req.value, StandardCharsets.UTF_8));

        if (req.ack == 0 || req.ack == 1) {
            replicateAsync(req.topic, targetPartition, offset, req.value);
        } else {
            try {
                replicateSync(req.topic, targetPartition, offset, req.value);
                List<String> isr = getInSyncReplicas(req.topic, targetPartition, offset);
                System.out.println("ack=all: confirmed ISR members: " + isr);
            } catch (InterruptedException e) {
                throw new IOException("Replication wait interrupted", e);
            }
        }
        return new ProduceResponse(true, offset, null);
    }

    private void replicateAsync(String topic, int partition, long offset, byte[] value) {
       for(String peer : allPeers) {
           replicationQueues.computeIfAbsent(peer, p -> {
               BlockingQueue<ReplicationTask> queue = new LinkedBlockingQueue<>();
               Thread senderThread = new Thread(() -> runReplicationSender(p, queue));
               senderThread.setDaemon(true);
               senderThread.start();
               return queue;
           }).offer(new ReplicationTask(topic, partition, offset, value));
       }
    }

    private void replicateSync(String topic, int partition, long offset, byte[] value) throws IOException, InterruptedException {
        replicateAsync(topic, partition, offset, value);
        for (String peer : allPeers) {
            String key = topic + "-" + partition;
            while (followerOffsets.getOrDefault(key, Collections.emptyMap()).getOrDefault(peer, -1L) < offset) {
                Thread.sleep(5);
            }
        }
    }



    private List<String> getInSyncReplicas(String topic, int partition, long leaderOffset) {
        String key = topic + "-" + partition;
        Map<String, Long> offsets = followerOffsets.getOrDefault(key, Collections.emptyMap());

        List<String> inSync = new ArrayList<>();
        for (String address : allPeers) {
            long followerOffset = offsets.getOrDefault(address, -1L);
            if (leaderOffset - followerOffset <= maxLag) {
                inSync.add(address);
            }
        }
        return inSync;
    }

    private synchronized JoinGroupResponse handleJoinGroup(JoinGroupRequest req) {
        if (raftState.getRole() != RaftState.NodeRole.LEADER) {
            String hint = raftState.getCurrentLeaderId();
            System.out.println("NOT_LEADER (JoinGroup) - redirecting to: " + hint);
            return new JoinGroupResponse(false, null, hint);
        }

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

        return new JoinGroupResponse(true, assigned, null);
    }

    private void onElectionTimeout() {
        if(raftState.getRole() == RaftState.NodeRole.LEADER) {
            return;
        }

        int newTerm = raftState.startElection();
        System.out.println("Election timeout! Starting election for term " + newTerm);

        int votesReceived = 1; // already voted for self
        int majorityNeeded = (allPeers.size() + 1) / 2 + 1;

        for(String peer : allPeers) {
            try {
                RequestVoteRequest req = new RequestVoteRequest(newTerm, raftState.getSelfId());
                Frame frame = new Frame(MessageType.REQUEST_VOTE, req.encode());

                DataOutputStream out = getOrConnectPeer(peer);
                DataInputStream in = peerInputs.get(peer);

                frame.writeTo(out);
                Frame responseFrame = Frame.readFrom(in);
                RequestVoteResponse resp = RequestVoteResponse.decode(responseFrame.payload());

                if (resp.term > raftState.getCurrentTerm()) {
                    raftState.becomeFollower(resp.term);
                    System.out.println("Saw higher term from " + peer + ", stepping down");
                    return;
                }

                if (resp.voteGranted) {
                    votesReceived++;
                    System.out.println("Got vote from " + peer + ", total votes: " + votesReceived);
                }
            } catch (IOException e) {
                System.out.println("Could not reach peer " + peer + " for vote: " + e.getMessage());
            }
        }

        if (votesReceived >= majorityNeeded && raftState.getRole() == RaftState.NodeRole.CANDIDATE) {
            raftState.becomeLeader();
            startHeartbeats();
        } else {
            electionTimer.reset();
        }
    }

    private void startHeartbeats() {
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            for(String peer : allPeers) {
                try {
                    HeartbeatRequest req = new HeartbeatRequest(raftState.getCurrentTerm(), raftState.getSelfId());
                    Frame frame = new Frame(MessageType.HEARTBEAT, req.encode());
                    DataOutputStream out = getOrConnectPeer(peer);
                    DataInputStream in = peerInputs.get(peer);
                    frame.writeTo(out);
                    Frame.readFrom(in);
                } catch (IOException e) {
//                    System.out.println("Heartbeat to " + peer + " failed: " + e.getMessage());
                }
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) throws IOException {
//        int port = Integer.parseInt(args[0]);
//
//        List<String> followerAddresses = new ArrayList<>();
//        if (!args[2].equals("-")) {
//            followerAddresses = Arrays.asList(args[2].split(","));
//        }
//
//        List<String> allPeers = Arrays.asList(args[3].split(","));
//
//        BrokerServer broker = new BrokerServer(port, followerAddresses, allPeers);
//        broker.start();
        int port = Integer.parseInt(args[0]);
        List<String> allPeers = Arrays.asList(args[1].split(","));

        BrokerServer broker = new BrokerServer(port, allPeers);
        broker.start();
    }
}