package com.pr_reviewer.queueforge.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Frame {
    private final MessageType type;
    private final byte[] payload;

    public Frame(MessageType type, byte[] payload) {
        this.type = type;
        this.payload = payload;
    }

    public MessageType type() {
        return type;
    }

    public byte[] payload() {
        return payload;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        int length = 1 + payload.length;
        out.writeInt(length);
        out.writeByte(type.code());
        out.write(payload);
        out.flush();
    }

    public static Frame readFrom(DataInputStream in) throws IOException {
        int length = in.readInt();
        int typeCode = in.readUnsignedByte(); // 1byte
        int payloadLength = length - 1;
        byte[] payload = new byte[payloadLength];
        in.readFully(payload);
        return new Frame(MessageType.fromCode(typeCode), payload);
    }
}
