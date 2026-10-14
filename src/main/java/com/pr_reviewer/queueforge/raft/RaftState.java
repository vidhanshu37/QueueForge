package com.pr_reviewer.queueforge.raft;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftState {

    public enum NodeRole { FOLLOWER, CANDIDATE, LEADER }

    private volatile NodeRole role = NodeRole.FOLLOWER;
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private volatile String votedFor = null;
    private final String selfId;
    private volatile String currentLeaderId = null;
    private final File stateFile;

    public synchronized String getCurrentLeaderId() {
        return currentLeaderId;
    }

    public synchronized void setCurrentLeaderId(String leaderId) {
        this.currentLeaderId = leaderId;
    }

    public RaftState(String selfId) {
        this(selfId, null);
    }

    public RaftState(String selfId, File stateFile) {
        this.selfId = selfId;
        this.stateFile = stateFile;
        loadFromDisk();
    }

    public synchronized NodeRole getRole() {
        return role;
    }

    public synchronized int getCurrentTerm() {
        return currentTerm.get();
    }

    public String getSelfId() {
        return selfId;
    }

    public synchronized int startElection() {
        currentLeaderId = null;
        role = NodeRole.CANDIDATE;
        int newTerm = currentTerm.incrementAndGet();
        votedFor = selfId;
        persistOrThrow();
        System.out.println("[" + selfId + "] Starting election for term " + newTerm);
        return newTerm;
    }

    public synchronized void becomeLeader() {

        if (role == NodeRole.CANDIDATE) {
            role = NodeRole.LEADER;
            currentLeaderId = selfId;
            System.out.println("[" + selfId + "] Became LEADER for term " + currentTerm.get());
        }
    }

    public synchronized void becomeFollower(int term) {
        if (term > currentTerm.get()) {
            currentTerm.set(term);
            votedFor = null;
            persistOrThrow();
        }
        role = NodeRole.FOLLOWER;
    }

    public synchronized boolean handleVoteRequest(int candidateTerm, String candidateId) {
        return handleVoteRequest(candidateTerm, candidateId, true);
    }


    public synchronized boolean handleVoteRequest(int candidateTerm, String candidateId, boolean candidateUpToDate) {
        if (candidateTerm < currentTerm.get()) {
            return false;
        }
        boolean changed = false;
        if (candidateTerm > currentTerm.get()) {
            currentTerm.set(candidateTerm);
            votedFor = null;
            role = NodeRole.FOLLOWER;
            changed = true;
        }
        boolean grant = candidateUpToDate && (votedFor == null || votedFor.equals(candidateId));
        if (grant && !candidateId.equals(votedFor)) {
            votedFor = candidateId;
            changed = true;
        }
        if (changed) {
            try {
                persist();
            } catch (IOException e) {
                System.out.println("[" + selfId + "] RAFT state save failed, vote refused: " + e.getMessage());
                return false;
            }
        }
        if (grant) {
            System.out.println("[" + selfId + "] Voted for " + candidateId + " in term " + candidateTerm);
        }
        return grant;
    }

    private void persistOrThrow() {
        try {
            persist();
        } catch (IOException e) {
            throw new UncheckedIOException("Raft state save failed", e);
        }
    }

    private void persist() throws IOException {
        if (stateFile == null) return;
        File dir = stateFile.getAbsoluteFile().getParentFile();
        dir.mkdirs();
        File tmp = new File(dir, stateFile.getName() + ".tmp");
        String content = "term=" + currentTerm.get() + "\nvotedFor=" + (votedFor == null ? "" : votedFor) + "\n";
        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            fos.write(content.getBytes(StandardCharsets.UTF_8));
            fos.getFD().sync();
        }
        Files.move(tmp.toPath(), stateFile.toPath(),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private void loadFromDisk() {
        if (stateFile == null || !stateFile.exists()) return;
        try {
            int term = 0;
            String voted = null;
            for (String line : Files.readAllLines(stateFile.toPath(), StandardCharsets.UTF_8)) {
                if (line.startsWith("term=")) {
                    term = Integer.parseInt(line.substring(5).trim());
                } else if (line.startsWith("votedFor=")) {
                    String v = line.substring(9).trim();
                    voted = v.isEmpty() ? null : v;
                }
            }
            currentTerm.set(term);
            votedFor = voted;
            System.out.println("[" + selfId + "] Restored raft state: term=" + term + ", votedFor=" + voted);
        } catch (IOException | NumberFormatException e) {
            throw new IllegalStateException("Corrupt raft state file " + stateFile + ": " + e.getMessage(), e);
        }
    }
}