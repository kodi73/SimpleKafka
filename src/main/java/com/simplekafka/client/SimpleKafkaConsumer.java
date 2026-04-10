package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SimpleKafkaConsumer provides a high-level API for reading messages from a topic.
 *
 * It wraps SimpleKafkaClient and adds:
 *  - Automatic offset tracking (remembers where it left off)
 *  - A continuous polling loop with idle backoff
 *  - A callback-based interface (you provide a MessageHandler, we call it for each message)
 *  - Seek functionality (jump to any offset, e.g., replay from the beginning)
 *
 * Design note: In real Kafka, consumers belong to consumer groups —
 * multiple consumers coordinate to split partitions between them.
 * We implement a simpler single-consumer model here.
 */
public class SimpleKafkaConsumer {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());

    // How many bytes to request per fetch call.
    // 1MB is a reasonable default — large enough for many messages,
    // small enough not to overwhelm the network.
    private static final int MAX_FETCH_BYTES = 1024 * 1024; // 1MB

    // How long to wait between polls when there are no new messages.
    // Without this, we'd hammer the broker with thousands of empty
    // requests per second — wasteful and potentially harmful.
    private static final long POLL_INTERVAL_MS = 100; // 100 milliseconds

    // ----------------------------------------------------------------
    // Core state
    // ----------------------------------------------------------------

    private final SimpleKafkaClient client;
    private final String topic;
    private final int    partition;

    // The offset of the NEXT message to fetch.
    // Starts at 0 (beginning of partition).
    // Advances by 1 for each message successfully processed.
    //
    // This is the consumer's "bookmark" — if it restarts, it resumes here.
    // In real Kafka, this offset is committed to a special internal topic
    // so it survives consumer restarts.
    private long currentOffset;

    // ----------------------------------------------------------------
    // Background thread state
    // ----------------------------------------------------------------

    // Controls the polling loop — set to false to stop consuming
    private final AtomicBoolean running;

    // The background thread that continuously polls for messages
    private Thread consumerThread;

    // ----------------------------------------------------------------
    // Message handler interface
    // ----------------------------------------------------------------

    /**
     * Applications implement this interface to process messages.
     *
     * The consumer calls handle() for every message it receives.
     * The handler runs inside the consumer's background thread,
     * so it should be fast and not block for long.
     *
     * Example usage:
     *   consumer.startConsuming((message, offset) -> {
     *       System.out.println("Got message at offset " + offset + ": " + message);
     *   });
     */
    public interface MessageHandler {
        /**
         * @param message the raw message bytes
         * @param offset  the offset of this message in the partition
         */
        void handle(byte[] message, long offset);
    }

    // ----------------------------------------------------------------
    // Constructor
    // ----------------------------------------------------------------

    /**
     * Create a consumer for a specific topic-partition.
     *
     * In real Kafka, consumers can subscribe to entire topics and
     * the partition assignment is automatic. Here we explicitly
     * choose a partition for simplicity.
     *
     * @param bootstrapHost any broker's hostname for initial metadata
     * @param bootstrapPort that broker's port
     * @param topic         topic to consume from
     * @param partition     specific partition to read from
     */
    public SimpleKafkaConsumer(String bootstrapHost, int bootstrapPort,
                                String topic, int partition) {
        this.client        = new SimpleKafkaClient(bootstrapHost, bootstrapPort);
        this.topic         = topic;
        this.partition     = partition;
        this.currentOffset = 0; // Start from the beginning of the partition
        this.running       = new AtomicBoolean(false);
    }

    /**
     * Initialize the consumer by fetching cluster metadata.
     * Must be called before poll() or startConsuming().
     */
    public void initialize() throws IOException {
        client.initialize();
        LOGGER.info("Consumer initialized for topic=" + topic + " partition=" + partition
                    + " starting at offset=" + currentOffset);
    }

    // ----------------------------------------------------------------
    // Manual polling
    // ----------------------------------------------------------------

    /**
     * Fetch the next batch of messages from the broker.
     *
     * This is the manual polling method — the application calls it
     * in its own loop. For automatic background polling, use startConsuming().
     *
     * After a successful poll:
     *   - The returned list contains the messages in order
     *   - currentOffset is advanced past all returned messages
     *   - The next call to poll() will fetch the messages AFTER these
     *
     * If no new messages exist at currentOffset, returns an empty list.
     * The caller should wait a bit before calling poll() again.
     *
     * @return list of raw message byte arrays (may be empty)
     */
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic, partition, currentOffset, MAX_FETCH_BYTES);

        if (!messages.isEmpty()) {
            // Advance the offset by however many messages we received.
            // This means: "I've processed these, give me the ones after them next time."
            currentOffset += messages.size();
            LOGGER.info("Polled " + messages.size() + " messages. "
                        + "New offset: " + currentOffset);
        }

        return messages;
    }

    // ----------------------------------------------------------------
    // Automatic background consumption
    // ----------------------------------------------------------------

    /**
     * Start consuming messages in a background thread.
     *
     * This is the "set it and forget it" consumption model.
     * You provide a MessageHandler callback — we call it for every message.
     *
     * The background thread:
     *  1. Calls poll() to fetch a batch of messages
     *  2. Calls handler.handle() for each message with its offset
     *  3. If no messages, waits POLL_INTERVAL_MS before trying again
     *  4. Repeats until stopConsuming() is called
     *
     * compareAndSet(false, true) is an atomic operation that:
     *  - Checks if running == false
     *  - If yes, sets it to true and returns true (we start)
     *  - If no (already true), returns false (already running, don't start again)
     * This prevents accidentally starting two consumer threads.
     */
    public void startConsuming(MessageHandler handler) {
        // Only start if not already running
        if (!running.compareAndSet(false, true)) {
            LOGGER.warning("Consumer is already running");
            return;
        }

        consumerThread = new Thread(() -> {
            LOGGER.info("Consumer thread started for topic=" + topic
                        + " partition=" + partition);

            while (running.get()) {
                try {
                    List<byte[]> messages = poll();

                    if (!messages.isEmpty()) {
                        // Calculate the starting offset for this batch.
                        // currentOffset was already advanced in poll(),
                        // so we go back by messages.size() to find the first one.
                        long batchStartOffset = currentOffset - messages.size();

                        // Call the handler for each message with its correct offset
                        for (int i = 0; i < messages.size(); i++) {
                            long messageOffset = batchStartOffset + i;
                            try {
                                handler.handle(messages.get(i), messageOffset);
                            } catch (Exception e) {
                                // Don't let a bad handler crash the consumer thread
                                LOGGER.log(Level.WARNING,
                                    "Handler threw exception for offset " + messageOffset, e);
                            }
                        }
                    } else {
                        // No messages available — back off to avoid busy-spinning
                        // This is called "exponential backoff" in more sophisticated systems
                        Thread.sleep(POLL_INTERVAL_MS);
                    }

                } catch (InterruptedException e) {
                    // stopConsuming() interrupted our sleep — clean exit
                    Thread.currentThread().interrupt();
                    break;
                } catch (IOException e) {
                    if (running.get()) {
                        LOGGER.log(Level.SEVERE, "Error fetching messages", e);
                        // Brief pause before retrying to avoid hammering a struggling broker
                        try { Thread.sleep(POLL_INTERVAL_MS * 5); }
                        catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }

            LOGGER.info("Consumer thread stopped for topic=" + topic
                        + " partition=" + partition);
        });

        // Daemon thread — automatically dies when the JVM shuts down.
        // This means you don't need to explicitly call stopConsuming()
        // when your application exits.
        consumerThread.setDaemon(true);
        consumerThread.setName("consumer-" + topic + "-" + partition);
        consumerThread.start();

        LOGGER.info("Started consuming from topic=" + topic + " partition=" + partition);
    }

    /**
     * Stop the background consumption loop.
     *
     * Sets running=false, then interrupts the consumer thread.
     * The interrupt wakes it up if it's sleeping in the backoff wait.
     * Then we join() to wait for it to finish cleanly.
     *
     * compareAndSet(true, false) is the reverse of the start check —
     * only proceeds if we were actually running.
     */
    public void stopConsuming() {
        if (!running.compareAndSet(true, false)) {
            return; // Wasn't running
        }

        if (consumerThread != null) {
            consumerThread.interrupt(); // Wake it up if sleeping

            try {
                // Wait up to 2 seconds for the thread to finish cleanly
                consumerThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        LOGGER.info("Stopped consuming from topic=" + topic + " partition=" + partition);
    }

    // ----------------------------------------------------------------
    // Offset management
    // ----------------------------------------------------------------

    /**
     * Jump to a specific offset.
     *
     * This is powerful — it lets you:
     *  - Replay from the beginning: seek(0)
     *  - Skip to the latest: seek(partition.getNextOffset())
     *  - Replay from a specific point in time (if you store offset→timestamp)
     *
     * In real Kafka this is called seekToBeginning(), seekToEnd(), or seek(offset).
     */
    public void seek(long offset) {
        LOGGER.info("Seeking to offset " + offset
                    + " (was at " + currentOffset + ")");
        this.currentOffset = offset;
    }

    /**
     * Returns the current offset — the next message that will be fetched.
     */
    public long getCurrentOffset() {
        return currentOffset;
    }

    /**
     * Convenience method: convert a raw message byte array to a UTF-8 string.
     *
     * Since we encode strings as UTF-8 in the producer, we decode the same way here.
     * This is the "deserialization" counterpart to the producer's serialization.
     */
    public static String messageToString(byte[] message) {
        return new String(message, StandardCharsets.UTF_8);
    }

    /**
     * Clean up resources.
     */
    public void close() {
        stopConsuming();
        LOGGER.info("Consumer closed");
    }
}