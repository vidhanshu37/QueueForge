package com.pr_reviewer.queueforge.broker;

import com.pr_reviewer.queueforge.protocol.*;
import com.pr_reviewer.queueforge.raft.ElectionTimer;
import com.pr_reviewer.queueforge.raft.RaftState;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BrokerServer {

    private final Map<String, PeerConn> controlConns = new ConcurrentHashMap<>();
    private final Map<String, PeerConn> dataConns = new ConcurrentHashMap<>();
    private static final long SESSION_TIMEOUT_MS = 10_000; // member timout duration

    private static class GroupState {
        int generation = 0;
        final TreeMap<String, Long> members = new TreeMap<>();
    }

    private final Map<String, GroupState> consumerGroups = new ConcurrentHashMap<>();

    private static class PeerConn {
        final Socket socket;
        final DataInputStream in;
        final DataOutputStream out;

        PeerConn(Socket socket) throws IOException {
            this.socket = socket;
            socket.setSoTimeout(2000);
            socket.setTcpNoDelay(true);
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        }

        void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private PeerConn getConn(Map<String, PeerConn> pool, String peer) throws IOException {
        PeerConn conn = pool.get(peer);
        if (conn != null) return conn;

        String[] parts = peer.split(":");
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])), 1000);
        conn = new PeerConn(socket);

        PeerConn existing = pool.putIfAbsent(peer, conn);
        if (existing != null) {
            conn.close();
            return existing;
        }
        return conn;
    }

    private Frame roundTrip(Map<String, PeerConn> pool, String peer, Frame request) throws IOException {
        PeerConn conn = getConn(pool, peer);
        synchronized (conn) {
            try {
                request.writeTo(conn.out);
                return Frame.readFrom(conn.in);
            } catch (IOException e) {
                pool.remove(peer, conn);
                conn.close();
                throw e;
            }
        }
    }

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
//    private final Map<String, Socket> peerSockets = new ConcurrentHashMap<>();
//    private final Map<String, DataOutputStream> peerOutputs = new ConcurrentHashMap<>();
//    private final Map<String, DataInputStream> peerInputs = new ConcurrentHashMap<>();

    private static final int MIN_INSYNC_REPLICAS = 2;
    private static final long ACK_ALL_TIMEOUT_MS = 5000;
    private static final int MAX_ISR_LAG = 5;
    private final Map<String, Set<String>> isr = new ConcurrentHashMap<>();
    private final Set<String> downPeers = ConcurrentHashMap.newKeySet();
    private final Map<String, Object> ackMonitors = new ConcurrentHashMap<>();
    private final AtomicInteger ackWaiters = new AtomicInteger();
    private final Set<String> probesQueued = ConcurrentHashMap.newKeySet();

    private RaftState raftState;
    private ElectionTimer electionTimer;

    private final Map<String, BlockingQueue<ReplicationTask>> replicationQueues = new ConcurrentHashMap<>();

    private static final String OFFSETS_TOPIC = "__consumer_offsets";
    private final Map<String, Long> committedOffsets = new ConcurrentHashMap<>();
    private volatile boolean offsetsReady = false;
//    private final Map<String, LinkedHashMap<String, Long>> consumerGroups = new ConcurrentHashMap<>();

    private final Map<String, Map<String, Long>> followerOffsets = new ConcurrentHashMap<>();


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

//    private List<String> getCurrentFollowers() {
//        return allPeers;
//    }

//    private DataOutputStream getOrConnectPeer(String peerAddress) throws IOException {
//        if (peerOutputs.containsKey(peerAddress)) {
//            return peerOutputs.get(peerAddress);
//        }
//        String[] parts = peerAddress.split(":");
//        Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
//        peerSockets.put(peerAddress, socket);
//        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
//        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
//        peerOutputs.put(peerAddress, out);
//        peerInputs.put(peerAddress, in);
//        return out;
//    }

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
//            int hash = java.util.Arrays.hashCode(key);
//            return Math.floorMod(hash, partitions.size());
            return Partitioner.partitionFor(key, partitions.size());
        }
        return new Random().nextInt(partitions.size());
    }

    private void loadExistingTopics() {
        File[] dirs = new File("data/data-" + port).listFiles(File::isDirectory);
        if (dirs == null) return;
        for (File d : dirs) {
            String name = d.getName();
            int dash = name.lastIndexOf('-');
            if (dash <= 0) continue;
            getOrCreatePartitions(name.substring(0, dash));
        }
    }

    private boolean isCandidateUpToDate(String candidateId, Map<String, Long> candidateEnds) {
        for (Map.Entry<String, Long> mine : localLogEnds().entrySet()) {
            long theirs = candidateEnds.getOrDefault(mine.getKey(), 0L);
            if (theirs < mine.getValue()) {
                System.out.println("[VOTE] rejecting " + candidateId + ": behind on " + mine.getKey()
                        + " (candidate " + theirs + " < mine " + mine.getValue() + ")");
                return false;
            }
        }
        return true;
    }

    private Map<String, Long> localLogEnds() {
        Map<String, Long> ends = new HashMap<>();
        for (Map.Entry<String, List<Partition>> e : topicPartitions.entrySet()) {
            List<Partition> ps = e.getValue();
            for (int i = 0; i < ps.size(); i++) {
                ends.put(e.getKey() + "-" + i, ps.get(i).size());
            }
        }
        return ends;
    }

    public void start() throws IOException {

        loadExistingTopics();
        raftState = new RaftState("localhost:" + port, new File("data/data-" + port + "/raft-state"));
        electionTimer = new ElectionTimer(3000, 5000, this::onElectionTimeout);
        electionTimer.reset();

        ServerSocket serverSocket = new ServerSocket(port);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "shutdown-hook"));
        System.out.println("Broker server started on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("Accepted connection from " + clientSocket.getRemoteSocketAddress());
            Thread handlerThread = new Thread(() -> handleClient(clientSocket));
            handlerThread.start();
        }
    }

    private void shutdown() {
        System.out.println("Shutting down broker " + port + "...");
        if (electionTimer != null)
            electionTimer.stop();

        if (heartbeatScheduler != null) heartbeatScheduler.shutdownNow();

        for (PeerConn c : controlConns.values())
            c.close();

        for (PeerConn c : dataConns.values())
            c.close();

        for (List<Partition> partitions : topicPartitions.values()) {
            for (Partition p : partitions) {
                try {
                    p.close();
                } catch (IOException e) {
                    System.out.println("Error closing partition: " + e.getMessage());
                }
            }
        }
        System.out.println("Shutdown complete.");
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
                    Partition p = getOrCreatePartitions(repReq.topic).get(repReq.partition);

                    boolean ok;
                    if (repReq.offset < p.size()) {
                        ok = true;
                    } else {
                        try {
                            p.appendAt(repReq.offset, repReq.value);
                            ok = true;
                        } catch (IllegalStateException ex) {
                            ok = false;
                            System.out.println("[FOLLOWER] gap on " + repReq.topic + "-" + repReq.partition
                                    + ": have " + p.size() + ", got " + repReq.offset);
                        }
                    }

                    byte[] ackPayload = ByteBuffer.allocate(9).put((byte) (ok ? 1 : 0)).putLong(p.size()).array();

                    new Frame(MessageType.REPLICATE_RESPONSE, ackPayload).writeTo(out);

                } else if (frame.type() == MessageType.REQUEST_VOTE) {
                    RequestVoteRequest req = RequestVoteRequest.decode(frame.payload());
                    boolean upToDate = isCandidateUpToDate(req.candidateId, req.logEnds);
                    boolean granted = raftState.handleVoteRequest(req.term, req.candidateId, upToDate);
                    RequestVoteResponse resp = new RequestVoteResponse(granted, raftState.getCurrentTerm());
                    Frame response = new Frame(MessageType.REQUEST_VOTE_RESPONSE, resp.encode());
                    response.writeTo(out);

                } else if (frame.type() == MessageType.LIST_OFFSETS) {
                    ListOffsetsRequest req = ListOffsetsRequest.decode(frame.payload());
                    ListOffsetsResponse resp = handleListOffsets(req);
                    new Frame(MessageType.LIST_OFFSETS_RESPONSE, resp.encode()).writeTo(out);
                } else if (frame.type() == MessageType.OFFSET_COMMIT) {
                    OffsetCommitRequest req = OffsetCommitRequest.decode(frame.payload());
                    OffsetCommitResponse resp = handleOffsetCommit(req);
                    new Frame(MessageType.OFFSET_COMMIT_RESPONSE, resp.encode()).writeTo(out);
                } else if (frame.type() == MessageType.OFFSET_FETCH) {
                    OffsetFetchRequest req = OffsetFetchRequest.decode(frame.payload());
                    OffsetFetchResponse resp = handleOffsetFetch(req);
                    new Frame(MessageType.OFFSET_FETCH_RESPONSE, resp.encode()).writeTo(out);
                } else if (frame.type() == MessageType.GROUP_HEARTBEAT) {
                    GroupHeartbeatRequest req = GroupHeartbeatRequest.decode(frame.payload());
                    GroupHeartbeatResponse resp = handleGroupHeartbeat(req);
                    new Frame(MessageType.GROUP_HEARTBEAT_RESPONSE, resp.encode()).writeTo(out);
                } else {
                    System.out.println("Received unknown message type: " + frame.type());
                }
            }
        } catch (EOFException e) {
            System.out.println("Client disconnected: " + socket.getRemoteSocketAddress());
        } catch (IOException e) {
            System.out.println("Connection error: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }



    private ListOffsetsResponse handleListOffsets(ListOffsetsRequest req) {
        List<Partition> partitions = getOrCreatePartitions(req.topic);
        if (req.partition < 0 || req.partition >= partitions.size()) {
            return new ListOffsetsResponse(ListOffsetsResponse.ERR_UNKNOWN_PARTITION, partitions.size(), -1, -1);
        }
        Partition p = partitions.get(req.partition);
        return new ListOffsetsResponse(ListOffsetsResponse.ERR_NONE, partitions.size(),
                p.earliestOffset(), p.size());
    }

    private FetchResponse handleFetch(FetchRequest req) throws IOException {
        List<Partition> partitions = getOrCreatePartitions(req.topic);
        if (req.partition < 0 || req.partition >= partitions.size()) {
            return FetchResponse.error(FetchResponse.ERR_UNKNOWN_PARTITION, -1, -1);
        }
        Partition partition = partitions.get(req.partition);
        long earliest = partition.earliestOffset();
        long latest = partition.size();

        if (req.offset < earliest || req.offset > latest) {
            System.out.println("[FETCH] OUT_OF_RANGE " + req.topic + "-" + req.partition
                    + " requested=" + req.offset + " earliest=" + earliest + " latest=" + latest);
            return FetchResponse.error(FetchResponse.ERR_OFFSET_OUT_OF_RANGE, earliest, latest);
        }
        if (req.offset == latest) {
            return FetchResponse.caughtUp(earliest, latest);
        }

        byte[] value = partition.read(req.offset);
        if (value == null) {
            long newEarliest = partition.earliestOffset();
            System.out.println("[FETCH] OUT_OF_RANGE (raced with retention) " + req.topic + "-" + req.partition
                    + " requested=" + req.offset + " earliest=" + newEarliest);
            return FetchResponse.error(FetchResponse.ERR_OFFSET_OUT_OF_RANGE, newEarliest, partition.size());
        }
        return FetchResponse.record(value, req.offset + 1, earliest, latest);
    }

    private ProduceResponse handleProduce(ProduceRequest req) throws IOException {
        if (raftState.getRole() != RaftState.NodeRole.LEADER) {
            String hint = raftState.getCurrentLeaderId();
            System.out.println("NOT_LEADER - redirecting producer to: " + hint);
            return new ProduceResponse(false, -1, ProduceResponse.ERR_NOT_LEADER, hint);
        }

        int targetPartition = selectPartition(req.topic, req.partition, req.key);
        boolean ackAll = req.ack != 0 && req.ack != 1;
        return appendReplicated(req.topic, targetPartition, req.value, ackAll);
    }

    private ProduceResponse appendReplicated(String topic, int targetPartition, byte[] value, boolean ackAll) throws IOException {
        List<Partition> partitions = getOrCreatePartitions(topic);
        String key = topic + "-" + targetPartition;

        if (ackAll && 1 + isrFor(key).size() < MIN_INSYNC_REPLICAS) {
            return new ProduceResponse(false, -1, ProduceResponse.ERR_NOT_ENOUGH_REPLICAS, null);
        }

        long offset = partitions.get(targetPartition).append(value);
        replicateAsync(topic, targetPartition, offset, value);

        if (ackAll) {
            try {
                awaitIsrAcks(key, offset);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Replication wait interrupted", e);
            }
            if (1 + isrFor(key).size() < MIN_INSYNC_REPLICAS) {
                return new ProduceResponse(false, -1, ProduceResponse.ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND, null);
            }
        }
        return new ProduceResponse(true, offset, null);
    }

    private OffsetCommitResponse handleOffsetCommit(OffsetCommitRequest req) throws IOException {
        if (raftState.getRole() != RaftState.NodeRole.LEADER) {
            return new OffsetCommitResponse(OffsetCommitResponse.ERR_NOT_LEADER, raftState.getCurrentLeaderId());
        }

        if (!offsetsReady) {
            return new OffsetCommitResponse(OffsetCommitResponse.ERR_NOT_READY, null);
        }

        int target = Partitioner.partitionFor(req.groupId.getBytes("UTF-8"), numPartitionPerTopic);

        ProduceResponse r = appendReplicated(OFFSETS_TOPIC, target, req.encode(), true);
        if (!r.success) {
            System.out.println("[OFFSETS] commit failed for " + req.mapKey() + " (errorCode=" + r.errorCode + ")");
            return new OffsetCommitResponse(OffsetCommitResponse.ERR_NOT_ENOUGH_REPLICAS, null);
        }

        committedOffsets.put(req.mapKey(), req.offset);
        System.out.println("[OFFSETS] committed " + req.mapKey() + " = " + req.offset);
        return new OffsetCommitResponse(OffsetCommitResponse.ERR_NONE, null);
    }

    private OffsetFetchResponse handleOffsetFetch(OffsetFetchRequest req) {
        if (raftState.getRole() != RaftState.NodeRole.LEADER) {
            return new OffsetFetchResponse(OffsetCommitResponse.ERR_NOT_LEADER, OffsetFetchResponse.NO_COMMIT,
                    raftState.getCurrentLeaderId());
        }
        if (!offsetsReady) {
            return new OffsetFetchResponse(OffsetCommitResponse.ERR_NOT_READY, OffsetFetchResponse.NO_COMMIT, null);
        }
        Long committed = committedOffsets.get(req.groupId + "|" + req.topic + "|" + req.partition);
        return new OffsetFetchResponse(OffsetCommitResponse.ERR_NONE,
                committed == null ? OffsetFetchResponse.NO_COMMIT : committed, null);
    }

    private void rebuildCommittedOffsets() {
        committedOffsets.clear();
        int records = 0;
        try {
            for (Partition p : getOrCreatePartitions(OFFSETS_TOPIC)) {
                long next = p.earliestOffset();
                while (next < p.size()) {
                    List<byte[]> batch = p.readBatch(next, 500);
                    if (batch.isEmpty()) break;
                    for (byte[] rec : batch) {
                        OffsetCommitRequest c = OffsetCommitRequest.decode(rec);
                        committedOffsets.put(c.mapKey(), c.offset);
                        records++;
                    }
                    next += batch.size();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not rebuild committed offsets", e);
        }
        System.out.println("[OFFSETS] rebuilt " + committedOffsets.size()
                + " committed offsets from " + records + " log records");
    }

    private void replicateAsync(String topic, int partition, long offset, byte[] value) {
        for (String peer : allPeers) {
            queueFor(peer).offer(new ReplicationTask(topic, partition, offset, value));
        }
    }

    private Object monitorFor(String key) {
        return ackMonitors.computeIfAbsent(key, k -> new Object());
    }

    private void notifyAcks(String key) {
        Object m = monitorFor(key);
        synchronized (m) {
            m.notifyAll();
        }
    }

    private boolean shrink(String key, String peer) {
        boolean removed = isrFor(key).remove(peer);
        if (removed) {
            System.out.println("[ISR] " + key + " - " + peer);
            notifyAcks(key);
        }
        return removed;
    }

    private void shrinkPeerEverywhere(String peer) {
        for (String key : isr.keySet()) {
            shrink(key, peer);
        }
    }

    private List<String> pendingIsr(String key, long offset) {
        Map<String, Long> acked = followerOffsets.getOrDefault(key, Collections.emptyMap());
        List<String> pending = new ArrayList<>();
        for (String peer : isrFor(key)) {
            if (acked.getOrDefault(peer, -1L) < offset) pending.add(peer);
        }
        return pending;
    }

    private void awaitIsrAcks(String key, long offset) throws InterruptedException {
        Object m = monitorFor(key);
        long deadline = System.currentTimeMillis() + ACK_ALL_TIMEOUT_MS;
        ackWaiters.incrementAndGet();
        try {
            synchronized (m) {
                while (true) {
                    List<String> pending = pendingIsr(key, offset);
                    if (pending.isEmpty()) return;
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        for (String peer : pending) shrink(key, peer);
                        return;
                    }
                    m.wait(remaining);
                }
            }
        } finally {
            ackWaiters.decrementAndGet();
        }
    }

    private void probeLaggingPeer(String peer) {
        for (Map.Entry<String, List<Partition>> e : topicPartitions.entrySet()) {
            for (int p = 0; p < e.getValue().size(); p++) {
                String key = e.getKey() + "-" + p;
                if (isrFor(key).contains(peer)) continue;
                if (!probesQueued.add(peer + "|" + key)) continue;
                queueFor(peer).offer(new ReplicationTask(e.getKey(), p, -1, null));
            }
        }
    }

    private void resetLeaderState() {
        isr.clear();
        followerOffsets.clear();
        downPeers.clear();
        probesQueued.clear();
        synchronized (this) {
            consumerGroups.clear();
        }
    }

    private Set<String> isrFor(String key) {
        return isr.computeIfAbsent(key, k -> {
            Set<String> members = ConcurrentHashMap.newKeySet();
            for (String peer : allPeers) {
                if (!downPeers.contains(peer)) members.add(peer);
            }
            return members;
        });
    }

    private BlockingQueue<ReplicationTask> queueFor(String peer) {
        return replicationQueues.computeIfAbsent(peer, p -> {
            BlockingQueue<ReplicationTask> queue = new LinkedBlockingQueue<>();
            Thread senderThread = new Thread(() -> runReplicationSender(p, queue));
            senderThread.setDaemon(true);
            senderThread.start();
            return queue;
        });
    }

    private void expireDeadMembers(String groupId, GroupState g) {
        long now = System.currentTimeMillis();
        boolean removed = false;
        Iterator<Map.Entry<String, Long>> it = g.members.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> e = it.next();
            if (now - e.getValue() > SESSION_TIMEOUT_MS) {
                it.remove();
                removed = true;
                System.out.println("[GROUP] '" + groupId + "' member " + e.getKey() + " expired (no heartbeat for "
                        + (now - e.getValue()) + " ms)");
            }
        }
        if (removed) {
            g.generation++;
            System.out.println("[GROUP] '" + groupId + "' generation -> " + g.generation + ", members=" + g.members.keySet());
        }
    }

    private synchronized JoinGroupResponse handleJoinGroup(JoinGroupRequest req) {
        if (raftState.getRole() != RaftState.NodeRole.LEADER) {
            String hint = raftState.getCurrentLeaderId();
            System.out.println("NOT_LEADER (JoinGroup) - redirecting to: " + hint);
            return new JoinGroupResponse(false, null, hint);
        }

        GroupState g = consumerGroups.computeIfAbsent(req.groupId, k -> new GroupState());
        expireDeadMembers(req.groupId, g);

        boolean isNewMember = !g.members.containsKey(req.consumerId);
        g.members.put(req.consumerId, System.currentTimeMillis());
        if (isNewMember) {
            g.generation++;
            System.out.println("[GROUP] '" + req.groupId + "' generation -> " + g.generation
                    + ", members=" + g.members.keySet());
        }

        List<String> memberList = new ArrayList<>(g.members.keySet());
        int myIndex = memberList.indexOf(req.consumerId);
        int totalMembers = memberList.size();

        List<Partition> partitions = getOrCreatePartitions(req.topic);
        List<Integer> assigned = new ArrayList<>();
        for (int p = 0; p < partitions.size(); p++) {
            if (p % totalMembers == myIndex) {
                assigned.add(p);
            }
        }

        System.out.println("Group '" + req.groupId + "' gen " + g.generation + " has " + totalMembers + " members. " +
                req.consumerId + " assigned partitions: " + assigned);

        return new JoinGroupResponse(true, assigned, null, g.generation);
    }

    private synchronized GroupHeartbeatResponse handleGroupHeartbeat(GroupHeartbeatRequest req) {
        if (raftState.getRole() != RaftState.NodeRole.LEADER) {
            return new GroupHeartbeatResponse(GroupHeartbeatResponse.ERR_NOT_LEADER, 0, raftState.getCurrentLeaderId());
        }
        GroupState g = consumerGroups.get(req.groupId);
        if (g != null) {
            expireDeadMembers(req.groupId, g);
        }
        if (g == null || !g.members.containsKey(req.consumerId)) {
            return new GroupHeartbeatResponse(GroupHeartbeatResponse.ERR_UNKNOWN_MEMBER, g == null ? 0 : g.generation, null);
        }
        g.members.put(req.consumerId, System.currentTimeMillis());
        if (req.generation != g.generation) {
            return new GroupHeartbeatResponse(GroupHeartbeatResponse.ERR_REBALANCE, g.generation, null);
        }
        return new GroupHeartbeatResponse(GroupHeartbeatResponse.ERR_NONE, g.generation, null);
    }

    private void onElectionTimeout() {

        if (raftState.getRole() == RaftState.NodeRole.LEADER) {
            return;

        }

        int newTerm = raftState.startElection();
        System.out.println("Election timeout! Starting election for term " + newTerm);

        int votesReceived = 1; // already voted for self
        int majorityNeeded = (allPeers.size() + 1) / 2 + 1;

        Map<String, Long> myLogEnds = localLogEnds();

        for (String peer : allPeers) {
            try {
                RequestVoteRequest req = new RequestVoteRequest(newTerm, raftState.getSelfId(), myLogEnds);
                Frame responseFrame = roundTrip(controlConns, peer,
                        new Frame(MessageType.REQUEST_VOTE, req.encode()));
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
            offsetsReady = false;
            raftState.becomeLeader();
            resetLeaderState();
            startHeartbeats();
            rebuildCommittedOffsets();
            offsetsReady = true;
        } else {
            electionTimer.reset();
        }
    }

    private static class ReplicateAck {
        final boolean ok;
        final long nextOffset;

        ReplicateAck(boolean ok, long nextOffset) {
            this.ok = ok;
            this.nextOffset = nextOffset;
        }
    }

    private ReplicateAck sendReplicate(String address, String topic, int partition, long offset, byte[] value) throws IOException {
        ReplicateRequest req = new ReplicateRequest(topic, partition, offset, value);
        Frame resp = roundTrip(dataConns, address, new Frame(MessageType.REPLICATE, req.encode()));
        ByteBuffer buf = ByteBuffer.wrap(resp.payload());
        boolean ok = buf.get() == 1;
        return new ReplicateAck(ok, buf.getLong());
    }

    private void markAcked(String topic, int partition, String address, long offset) {
        String key = topic + "-" + partition;
        followerOffsets.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                .merge(address, offset, Math::max);
        downPeers.remove(address);

        Set<String> members = isrFor(key);
        if (!members.contains(address)) {
            long leaderLast = getOrCreatePartitions(topic).get(partition).size() - 1;
            if (leaderLast - offset <= MAX_ISR_LAG && members.add(address)) {
                System.out.println("[ISR] " + key + " + " + address + " (caught up at " + offset + ")");
            }
        }
        if (ackWaiters.get() > 0) notifyAcks(key);
    }

    private void runReplicationSender(String address, BlockingQueue<ReplicationTask> queue) {
        while (true) {
            try {
                ReplicationTask task = queue.take();
                if (task.offset < 0) {
                    // probe: peer ko leader ka aakhri record bhejo. In-sync ho to duplicate-ok, peeche ho to gap -> catch-up
                    probesQueued.remove(address + "|" + task.topic + "-" + task.partition);
                    Partition leaderLog = getOrCreatePartitions(task.topic).get(task.partition);
                    long last = leaderLog.size() - 1;
                    if (last < 0) continue;
                    List<byte[]> rec = leaderLog.readBatch(last, 1);
                    if (rec.isEmpty()) continue;
                    replicateOne(address, task.topic, task.partition, last, rec.get(0));
                } else {
                    replicateOne(address, task.topic, task.partition, task.offset, task.value);
                }
            } catch (InterruptedException e) {
                break;
            } catch (IOException e) {
                if (downPeers.add(address)) {
                    System.out.println("[ISR] replication to " + address + " failed (" + e.getMessage() + ") -> removing from ISR");
                }
                shrinkPeerEverywhere(address);
            }
        }
    }

    private void replicateOne(String address, String topic, int partition, long offset, byte[] value) throws IOException {
        ReplicateAck ack = sendReplicate(address, topic, partition, offset, value);
        if (ack.ok) {
            markAcked(topic, partition, address, offset);
        } else {
            catchUp(address, topic, partition, ack.nextOffset);
        }
    }

    private void catchUp(String address, String topic, int partition, long from) throws IOException {
        Partition leaderLog = getOrCreatePartitions(topic).get(partition);
        String key = topic + "-" + partition;
        long cursor = from;
        System.out.println("[LEADER] catch-up " + address + " " + key + " from " + cursor + " to " + leaderLog.size());

        while (cursor < leaderLog.size()) {
            List<byte[]> batch = leaderLog.readBatch(cursor, 500);
            if (batch.isEmpty()) {
                System.out.println("[LEADER] catch-up aborted for " + address + " " + key + ": offset " + cursor + " leader log mein nahi hai (retention)");
                return;
            }
            for (byte[] value : batch) {
                ReplicateAck ack = sendReplicate(address, topic, partition, cursor, value);
                if (!ack.ok) {
                    if (ack.nextOffset == cursor) return;
                    cursor = ack.nextOffset;
                    break;
                }
                markAcked(topic, partition, address, cursor);
                cursor++;
            }
        }
        System.out.println("[LEADER] catch-up done for " + address + " " + key + " at " + cursor);
    }


    private void startHeartbeats() {
        if (heartbeatScheduler != null) heartbeatScheduler.shutdownNow();
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            if (raftState.getRole() != RaftState.NodeRole.LEADER) {
                heartbeatScheduler.shutdown();
                return;
            }
            for (String peer : allPeers) {
                try {
                    HeartbeatRequest req = new HeartbeatRequest(raftState.getCurrentTerm(), raftState.getSelfId());
                    roundTrip(controlConns, peer, new Frame(MessageType.HEARTBEAT, req.encode()));
                    probeLaggingPeer(peer);
                } catch (IOException e) {
                    System.out.println("Could not send heartbeat to " + peer + ": " + e.getMessage());
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