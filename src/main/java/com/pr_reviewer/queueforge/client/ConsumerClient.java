package com.pr_reviewer.queueforge.client;

import com.pr_reviewer.queueforge.protocol.FetchRequest;
import com.pr_reviewer.queueforge.protocol.FetchResponse;
import com.pr_reviewer.queueforge.protocol.Frame;
import com.pr_reviewer.queueforge.protocol.MessageType;

import java.io.*;
import java.net.Socket;

public class ConsumerClient {
    private final String host;
    private final int port;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    public ConsumerClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        System.out.println("Consumer connected to broker at " + host + ":" + port);
    }

    public FetchResponse fetch(String topic, int partition, long offset) throws IOException {
        FetchRequest req = new FetchRequest(topic, partition, offset);
        Frame frame = new Frame(MessageType.FETCH, req.encode());
        frame.writeTo(out);

        Frame responseFrame = Frame.readFrom(in);
        return FetchResponse.decode(responseFrame.payload());
    }

    public void pollLoop(String topic, int partition, long startOffset, int maxMessagesToRead) throws IOException, InterruptedException {
        long currentOffset = startOffset;
        int messagesRead = 0;

        while (messagesRead < maxMessagesToRead) {
            FetchResponse resp = fetch(topic, partition, currentOffset);

            if (resp.found) {
                String value = new String(resp.value, "UTF-8");
                System.out.println("Consumed from topic '" + topic + "' partition " + partition +
                        " at offset " + currentOffset + ": " + value);
                currentOffset = resp.nextOffset;
                messagesRead++;
            } else {
                System.out.println("No message at offset " + currentOffset + ", waiting...");
                Thread.sleep(1000);
            }
        }
    }

    public void close() throws IOException {
        socket.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ConsumerClient consumer = new ConsumerClient("localhost", 9092);
        consumer.connect();

        consumer.pollLoop("orders", 1, 0, 2);

        consumer.close();
    }



}
