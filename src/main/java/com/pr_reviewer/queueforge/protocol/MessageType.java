package com.pr_reviewer.queueforge.protocol;

public enum MessageType {
    PRODUCE(1),
    FETCH(2),
    PRODUCE_RESPONSE(3),
    FETCH_RESPONSE(4),
    REPLICATE(5),
    REPLICATE_RESPONSE(6),
    JOIN_GROUP(7),
    JOIN_GROUP_RESPONSE(8),
    REQUEST_VOTE(9),
    REQUEST_VOTE_RESPONSE(10),
    HEARTBEAT(11),
    HEARTBEAT_RESPONSE(12),
    LIST_OFFSETS(13),
    LIST_OFFSETS_RESPONSE(14),
    OFFSET_COMMIT(15),
    OFFSET_COMMIT_RESPONSE(16),
    OFFSET_FETCH(17),
    OFFSET_FETCH_RESPONSE(18),
    GROUP_HEARTBEAT(19),
    GROUP_HEARTBEAT_RESPONSE(20);


    private final int code;

    MessageType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static MessageType fromCode(int code) {
        for(MessageType t : values()) {
            if(t.code == code) return t;
        }

        throw new IllegalArgumentException("Unknown MessageType code: " + code);
    }
}
