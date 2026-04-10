package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SimpleKafkaProducer provides a high-level API for sending messages to a topic.
 *
 * It wraps SimpleKafkaClient and adds:
 *  - Automatic string serialization to bytes
 *  - Partition selection (random or key-based)
 *  - A simple send(message) interface applications can use directly
 *
 * Design note: In real Kafka, the producer also handles batching
 * (collecting many messages and sending them together for efficiency)
 * and compression. We keep it simple here.
 */
public class SimpleKafkaProducer {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaProducer.class.getName());

    // The low-level client that handles actual network communication
    private final SimpleKafkaClient client;

    // The topic this producer sends to.
    // One producer instance = one topic. For multiple topics, create multiple producers.
    private final String topic;

    // Tracks which partition gets the next message in round-robin mode.
    // Starts at 0, increments with each send, wraps around at partitionCount.
    private int roundRobinCounter;

    // ----------------------------------------------------------------
    // Constructor
    // ----------------------------------------------------------------

    /**
     * Create a producer that sends to a specific topic.
     *
     * @param bootstrapHost any broker's hostname — used for initial metadata fetch
     * @param bootstrapPort that broker's port
     * @param topic         the topic this producer will write to
     */
    public SimpleKafkaProducer(String bootstrapHost, int bootstrapPort, String topic) {
        this.client           = new SimpleKafkaClient(bootstrapHost, bootstrapPort);
        this.topic            = topic;
        this.roundRobinCounter = 0;
    }

    /**
     * Initialize the producer.
     *
     * This fetches cluster metadata so we know how many partitions
     * the topic has and which brokers lead them.
     * Must be called before any send().
     */
    public void initialize() throws IOException {
        client.initialize();
        LOGGER.info("Producer initialized for topic: " + topic);
    }

    // ----------------------------------------------------------------
    // Send methods
    // ----------------------------------------------------------------

    /**
     * Send a message to the topic using round-robin partition selection.
     *
     * Round-robin means:
     *   Message 1 → partition 0
     *   Message 2 → partition 1
     *   Message 3 → partition 2
     *   Message 4 → partition 0 (wraps around)
     *   ...
     *
     * This distributes load evenly across all partitions.
     * Use this when message ordering doesn't matter.
     *
     * @param message the string message to send
     * @return the offset assigned by the broker
     */
    public long send(String message) throws IOException {
        // Find out how many partitions this topic has
        int partitionCount = client.getPartitionCount(topic);

        if (partitionCount == 0) {
            // We don't know the topic — refresh metadata and try again
            client.refreshMetadata();
            partitionCount = client.getPartitionCount(topic);
            if (partitionCount == 0) {
                throw new IOException(
                    "Topic not found: " + topic + ". Has it been created?"
                );
            }
        }

        // Pick the next partition in round-robin order
        // The modulo (%) ensures we wrap around: e.g., 3 % 3 = 0
        int partition = roundRobinCounter % partitionCount;
        roundRobinCounter++;

        LOGGER.info("Sending via round-robin to partition " + partition);
        return send(message, partition);
    }

    /**
     * Send a message to a specific partition by key.
     *
     * Key-based routing means the same key ALWAYS goes to the same partition.
     * This is critical when you need ordering guarantees for related messages.
     *
     * Example: if key = "user-123", all events for user-123 go to the same
     * partition and will be consumed in order by a single consumer.
     *
     * How it works:
     *   partitionId = abs(key.hashCode()) % numberOfPartitions
     *
     * Java's String.hashCode() is deterministic — same string always
     * produces the same hash — so the partition assignment is stable.
     *
     * @param message the string message to send
     * @param key     the routing key (e.g., user ID, order ID)
     * @return the offset assigned by the broker
     */
    public long sendWithKey(String message, String key) throws IOException {
        int partitionCount = client.getPartitionCount(topic);

        if (partitionCount == 0) {
            client.refreshMetadata();
            partitionCount = client.getPartitionCount(topic);
            if (partitionCount == 0) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        // Math.abs() because hashCode() can be negative
        // and negative modulo in Java gives a negative result
        int partition = Math.abs(key.hashCode()) % partitionCount;

        LOGGER.info("Sending with key '" + key + "' to partition " + partition);
        return send(message, partition);
    }

    /**
     * Send a message to a specific, explicitly chosen partition.
     *
     * This is the actual send method — the other two call this one.
     *
     * Converts the string message to UTF-8 bytes, then delegates
     * to the low-level client.
     *
     * UTF-8 is used because it can represent any Unicode character
     * and is the standard encoding for text in network protocols.
     *
     * @param message   the string message to send
     * @param partition the exact partition to write to
     * @return the offset assigned by the broker
     */
    public long send(String message, int partition) throws IOException {
        // Convert string → bytes using UTF-8 encoding
        // This is the "serialization" step — turning a high-level object
        // into raw bytes the network can carry
        byte[] data = message.getBytes(StandardCharsets.UTF_8);

        return client.send(topic, partition, data);
    }

    /**
     * Clean up resources.
     * In a full implementation this would close connection pools.
     */
    public void close() {
        LOGGER.info("Producer closed for topic: " + topic);
    }
}