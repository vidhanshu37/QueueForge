package com.pr_reviewer.queueforge.client;

import com.pr_reviewer.queueforge.protocol.Frame;
import com.pr_reviewer.queueforge.protocol.MessageType;
import com.pr_reviewer.queueforge.protocol.ProduceRequest;
import com.pr_reviewer.queueforge.protocol.ProduceResponse;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

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

    public ProduceResponse send(String topic, int partition, byte[] key, byte[] value, int ack) throws IOException {
        ProduceRequest req = new ProduceRequest(topic, partition, key, value, ack);
        Frame frame = new Frame(MessageType.PRODUCE, req.encode());

        frame.writeTo(out);

        Frame responseFrame = Frame.readFrom(in);
        return ProduceResponse.decode(responseFrame.payload());
    }

    public void close() throws IOException {
        socket.close();
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: java ProducerClient <bootstrapServers> <topic>");
            return;
        }

        List<String> bootstrapServers = new ArrayList<>(Arrays.asList(args[0].split(",")));
        String topic = args[1];

        ProducerClient producer = connectToAny(bootstrapServers);

        System.out.println("=== Interactive Producer ===");
        System.out.println("Format: key:value OR value (key is optional)");
        System.out.println("Type 'exit' to quit.\n");

        Scanner scanner = new Scanner(System.in);
        System.out.print("> ");
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine().trim();

            if (line.equalsIgnoreCase("exit")) {
                break;
            }
            if (line.isEmpty()) {
                System.out.print("> ");
                continue;
            }

            String key = null;
            String value;
            if (line.contains(":")) {
                String[] parts = line.split(":", 2);
                key = parts[0];
                value = parts[1];
            } else {
                value = line;
            }

            byte[] keyBytes = (key != null) ? key.getBytes("UTF-8") : null;
            byte[] valueBytes = value.getBytes("UTF-8");

            boolean sent = false;
            int attempts = 0;
            int maxAttempts = bootstrapServers.size() + 2;

            while (!sent && attempts < maxAttempts) {
                attempts++;
                try {
                    ProduceResponse resp = producer.send(topic, -1, keyBytes, valueBytes, 1);

                    if (resp.success) {
                        System.out.println("Sent: " + value + " (offset=" + resp.offset + ")");
                        sent = true;
                    } else {
                        if (resp.leaderHint != null && !resp.leaderHint.isEmpty()) {
                            System.out.println("NOT_LEADER! Redirecting to: " + resp.leaderHint);
                            producer.close();
                            String[] hostPort = resp.leaderHint.split(":");
                            producer = new ProducerClient(hostPort[0], Integer.parseInt(hostPort[1]));
                            producer.connect();
                        } else {
                            System.out.println("No leader hint available (election in progress?), retrying via bootstrap list...");
                            producer.close();
                            producer = connectToAny(bootstrapServers);
                        }
                    }

                } catch (IOException e) {
                    System.out.println("Connection lost (" + e.getMessage() + "), trying another broker...");
                    try {
                        producer.close();
                    } catch (IOException ignored) {}
                    producer = connectToAny(bootstrapServers);
                }
            }

            if (!sent) {
                System.out.println("Failed to send after " + attempts + " attempts. Try again.");
            }

            System.out.print("> ");
        }

        producer.close();
        System.out.println("Producer closed.");
    }

    private static ProducerClient connectToAny(List<String> bootstrapServers) throws IOException {
        IOException lastError = null;
        for (String server : bootstrapServers) {
            try {
                String[] parts = server.split(":");
                ProducerClient client = new ProducerClient(parts[0], Integer.parseInt(parts[1]));
                client.connect();
                return client;
            } catch (IOException e) {
                lastError = e;
                System.out.println("Could not connect to " + server + ", trying next...");
            }
        }
        throw new IOException("No brokers reachable from bootstrap list", lastError);
    }
}