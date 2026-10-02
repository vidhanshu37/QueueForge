package com.pr_reviewer.queueforge;

import com.pr_reviewer.queueforge.protocol.ProduceRequest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class test {
    public static void main(String[] args) throws Exception {

        ProduceRequest req = new ProduceRequest("test-topic", 0, "user1".getBytes("UTF-8"), "order-placed".getBytes("UTF-8"));

        byte[] encode = req.encode();
        System.out.println("Encoded length: " + encode.length);

        ProduceRequest decode = ProduceRequest.decode(encode);

        System.out.println("Topic: " + decode.topic);
        System.out.println("Partition: " + decode.partition);
        System.out.println("Key: " + new String(decode.key, "UTF-8"));
        System.out.println("Value: " + new String(decode.value, "UTF-8"));
    }
}
