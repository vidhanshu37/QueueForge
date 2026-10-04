package com.pr_reviewer.queueforge.protocol;

public enum MessageType {
    PRODUCE(1),
    FETCH(2),
    PRODUCE_RESPONSE(3),
    FETCH_RESPONSE(4),
    REPLICATE(5),
    REPLICATE_RESPONSE(6),
    JOIN_GROUP(7),
    JOIN_GROUP_RESPONSE(8);

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
