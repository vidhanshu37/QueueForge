package com.pr_reviewer.queueforge.client;

import com.pr_reviewer.queueforge.protocol.Frame;
import com.pr_reviewer.queueforge.protocol.MessageType;
import com.pr_reviewer.queueforge.protocol.ProduceRequest;

import java.io.*;
import java.net.Socket;

public class ProducerClient {

    private final String host;
    private final int port;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;


    public ProducerClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        System.out.println("Connected to broker at " + host + ":" + port);
    }

    public void send(String topic, int partition, byte[] key, byte[] value, int ack) throws IOException {
        ProduceRequest req = new ProduceRequest(topic, partition, key, value, ack);
        byte[] payload = req.encode();
        Frame frame = new Frame(MessageType.PRODUCE, payload);

        frame.writeTo(out);

        Frame response = Frame.readFrom(in);
        String ackedTopic = new String(response.payload(), "UTF-8");
        System.out.println("Broker acked topic: " + ackedTopic);
    }

    public void close() throws IOException {
        socket.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ProducerClient producer = new ProducerClient("localhost", 9092);
        producer.connect();

        producer.send("orders", -1, "user123".getBytes("UTF-8"), "msg-A".getBytes("UTF-8"), 0);
        producer.send("orders", -1, "user123".getBytes("UTF-8"), "msg-B".getBytes("UTF-8"), 1);
        producer.send("orders", -1, "user001".getBytes("UTF-8"), "msg-1".getBytes("UTF-8"), 0);
        producer.send("orders", -1, "user001".getBytes("UTF-8"), "msg-2".getBytes("UTF-8"), 1);
        producer.send("orders", -1, "user001".getBytes("UTF-8"), "msg-3".getBytes("UTF-8"), -1);
        producer.send("orders", -1, "user001".getBytes("UTF-8"), "msg-4".getBytes("UTF-8"), 0);
        producer.send("orders", -1, "user002".getBytes("UTF-8"), "msg-D".getBytes("UTF-8"), 0);
        producer.close();
    }
}
