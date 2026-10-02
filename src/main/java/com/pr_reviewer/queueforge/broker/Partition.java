package com.pr_reviewer.queueforge.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Partition {
    private final List<byte[]> messages = Collections.synchronizedList(new ArrayList<>());

    public long append(byte[] value) {
        synchronized (messages) {
            messages.add(value);
            return messages.size() -1;
        }
    }

    public byte[] read(long offset) {
        synchronized (messages) {
            if(offset < 0 || offset >= messages.size()) {
                return null;
            }

            return messages.get((int)offset);
        }
    }

    public int size() {
        return messages.size();
    }
}
