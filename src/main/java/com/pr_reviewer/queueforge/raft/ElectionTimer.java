package com.pr_reviewer.queueforge.raft;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ElectionTimer {
    private final int minTimeoutMs;
    private final int maxTimeoutMs;
    private final Random random = new Random();
    private final Runnable onTimeout;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile ScheduledFuture<?> currentTask;

    public ElectionTimer(int minTimeoutMs, int maxTimeoutMs, Runnable onTimeout) {
        this.minTimeoutMs = minTimeoutMs;
        this.maxTimeoutMs = maxTimeoutMs;
        this.onTimeout = onTimeout;
    }

    private int randomDuration() {
        return minTimeoutMs + random.nextInt(maxTimeoutMs - minTimeoutMs);
    }

    public synchronized void reset() {
        if (currentTask != null) {
            currentTask.cancel(false);
        }
        int duration = randomDuration();
        currentTask = scheduler.schedule(onTimeout, duration, TimeUnit.MILLISECONDS);
    }


    public void stop() {
        if (currentTask != null) {
            currentTask.cancel(false);
        }
        scheduler.shutdown();
    }
}
