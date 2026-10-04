package com.pr_reviewer.queueforge.raft;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RaftState {

    public enum NodeRole { FOLLOWER, CANDIDATE, LEADER }

    private volatile NodeRole role = NodeRole.FOLLOWER;
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private volatile String votedFor = null;
    private final String selfId;

    public RaftState(String selfId) {
        this.selfId = selfId;
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
        role = NodeRole.CANDIDATE;
        int newTerm = currentTerm.incrementAndGet();
        votedFor = selfId;
        System.out.println("[" + selfId + "] Starting election for term " + newTerm);
        return newTerm;
    }

    public synchronized void becomeLeader() {
        if (role == NodeRole.CANDIDATE) {
            role = NodeRole.LEADER;
            System.out.println("[" + selfId + "] Became LEADER for term " + currentTerm.get());
        }
    }

    public synchronized void becomeFollower(int term) {
        if (term > currentTerm.get()) {
            currentTerm.set(term);
            votedFor = null;
        }
        role = NodeRole.FOLLOWER;
    }

    public synchronized boolean handleVoteRequest(int candidateTerm, String candidateId) {
        if (candidateTerm < currentTerm.get()) {
            return false;
        }
        if (candidateTerm > currentTerm.get()) {
            currentTerm.set(candidateTerm);
            votedFor = null;
            role = NodeRole.FOLLOWER;
        }
        if (votedFor == null || votedFor.equals(candidateId)) {
            votedFor = candidateId;
            System.out.println("[" + selfId + "] Voted for " + candidateId + " in term " + candidateTerm);
            return true;
        }
        return false;
    }
}