package com.simplekafka.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;

/**
 * SimpleKafkaClient is the low-level client for the SimpleKafka system.
 *
 * It handles:
 *  - Bootstrapping: connecting to any one broker to discover the full cluster
 *  - Metadata caching: remembering which broker leads which partition
 *  - Produce: sending a message to the correct leader broker for a partition
 *  - Fetch: reading messages from a broker starting at a given offset
 *
 * This is intentionally low-level. The higher-level SimpleKafkaProducer
 * and SimpleKafkaConsumer (Stage 7) wrap this with friendlier APIs.
 */
public class SimpleKafkaClient {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaClient.class.getName());

    // The initial broker we connect to just to discover the cluster.
    // After metadata is fetched, we may talk to completely different brokers.
    private final String bootstrapHost;
    private final int    bootstrapPort;

    // Cached cluster metadata:
    // Maps topic name → TopicMetadata (which contains partition → leader mappings)
    private final Map<String, TopicMetadata> topicMetadata;

    // Cached broker connection info:
    // Maps brokerId → BrokerInfo (host + port)
    // We need this to open connections to specific leader brokers.
    private final Map<Integer, BrokerInfo> brokerInfo;

    // ----------------------------------------------------------------
    // Constructor
    // ----------------------------------------------------------------

    public SimpleKafkaClient(String bootstrapHost, int bootstrapPort) {
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.topicMetadata = new ConcurrentHashMap<>();
        this.brokerInfo    = new ConcurrentHashMap<>();
    }

    // ----------------------------------------------------------------
    // Initialization
    // ----------------------------------------------------------------

    /**
     * Initialize the client by fetching cluster metadata from the bootstrap broker.
     *
     * This must be called before send() or fetch().
     * It populates our local caches so we know the full cluster topology.
     */
    public void initialize() throws IOException {
        LOGGER.info("Initializing client — connecting to bootstrap broker at "
                    + bootstrapHost + ":" + bootstrapPort);
        refreshMetadata();
        LOGGER.info("Client initialized. Known brokers: " + brokerInfo.size()
                    + ", Known topics: " + topicMetadata.size());
    }

    /**
     * Fetch fresh cluster metadata from the bootstrap broker.
     *
     * Steps:
     *  1. Open a TCP connection to the bootstrap broker
     *  2. Send a METADATA request (just 1 byte: 0x03)
     *  3. Read the response — it contains ALL brokers and ALL topic/partition info
     *  4. Populate our local caches
     *
     * We can call this again later if we get a "not the leader" error,
     * which means the cluster topology has changed and our cache is stale.
     */
    public void refreshMetadata() throws IOException {
        // Open a blocking connection to the bootstrap broker
        try (SocketChannel channel = SocketChannel.open(
                new InetSocketAddress(bootstrapHost, bootstrapPort))) {

            channel.configureBlocking(true);

            // Send the metadata request — just the single type byte 0x03
            ByteBuffer request = Protocol.encodeMetadataRequest();
            while (request.hasRemaining()) {
                channel.write(request);
            }

            // Read the response into a buffer.
            // Metadata can be large if there are many topics/brokers.
            ByteBuffer response = ByteBuffer.allocate(65536); // 64KB should be plenty
            channel.read(response);
            response.flip(); // Switch to read mode

            // Decode the metadata response using our Protocol class
            Protocol.MetadataResult result = Protocol.decodeMetadataResponse(response);

            if (!result.isSuccess()) {
                throw new IOException("Metadata request failed: " + result.error);
            }

            // Cache all broker connection details
            // We'll need these to open connections to specific leader brokers
            brokerInfo.clear();
            for (BrokerInfo broker : result.brokers) {
                brokerInfo.put(broker.getId(), broker);
                LOGGER.info("Discovered broker: " + broker);
            }

            // Cache all topic/partition metadata
            // This tells us which broker leads which partition
            topicMetadata.clear();
            for (Protocol.TopicMetadata topic : result.topics) {
                TopicMetadata tm = new TopicMetadata(topic.topicName);
                for (Protocol.PartitionMetadata pm : topic.partitions) {
                    tm.addPartition(pm.partitionId, pm.leaderId, pm.replicaIds);
                }
                topicMetadata.put(topic.topicName, tm);
                LOGGER.info("Discovered topic: " + topic.topicName
                            + " with " + topic.partitions.size() + " partitions");
            }
        }
    }

    // ----------------------------------------------------------------
    // Create Topic
    // ----------------------------------------------------------------

    /**
     * Request the controller to create a new topic.
     *
     * Sends a CREATE_TOPIC request to the bootstrap broker.
     * The bootstrap broker (if it's the controller) will create the topic
     * and notify all other brokers.
     *
     * After this call succeeds, we refresh metadata so our cache
     * includes the new topic's partition assignments.
     */
    public void createTopic(String topic, int numPartitions, short replicationFactor)
            throws IOException {
        LOGGER.info("Creating topic: " + topic + " partitions=" + numPartitions
                    + " replication=" + replicationFactor);

        try (SocketChannel channel = SocketChannel.open(
                new InetSocketAddress(bootstrapHost, bootstrapPort))) {

            channel.configureBlocking(true);

            ByteBuffer request = Protocol.encodeCreateTopicRequest(
                topic, numPartitions, replicationFactor
            );
            while (request.hasRemaining()) {
                channel.write(request);
            }

            // Read the acknowledgement
            ByteBuffer response = ByteBuffer.allocate(64);
            channel.read(response);
            response.flip();

            Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);
            if (!result.isSuccess()) {
                throw new IOException("Create topic failed: " + result.error);
            }

            LOGGER.info("Topic created successfully: " + topic);

            // Refresh our metadata cache so the new topic appears
            // Give the cluster a moment to propagate the topic to all brokers
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
            refreshMetadata();
        }
    }

    // ----------------------------------------------------------------
    // Send (Produce)
    // ----------------------------------------------------------------

    /**
     * Send a message to a specific topic-partition.
     *
     * Steps:
     *  1. Look up which broker is the leader for this partition
     *  2. Open a connection directly to that broker
     *  3. Send the PRODUCE request
     *  4. Read back the response (contains the assigned offset)
     *  5. Return the offset
     *
     * If our metadata is stale (e.g., the leader changed), we refresh
     * and retry once before giving up.
     *
     * @return the offset assigned to the message by the broker
     */
    public long send(String topic, int partition, byte[] message) throws IOException {
        LOGGER.info("Sending message to " + topic + "-" + partition
                    + " (" + message.length + " bytes)");

        // Find the leader broker for this partition
        BrokerInfo leader = findLeader(topic, partition);
        if (leader == null) {
            // Our metadata doesn't know about this topic/partition
            // Try refreshing and look again
            refreshMetadata();
            leader = findLeader(topic, partition);
            if (leader == null) {
                throw new IOException(
                    "Cannot find leader for " + topic + "-" + partition
                    + ". Is the topic created?"
                );
            }
        }

        // Open a direct connection to the leader broker
        try (SocketChannel channel = SocketChannel.open(
                new InetSocketAddress(leader.getHost(), leader.getPort()))) {

            channel.configureBlocking(true);

            // Encode and send the produce request
            ByteBuffer request = Protocol.encodeProduceRequest(topic, partition, message);
            while (request.hasRemaining()) {
                channel.write(request);
            }

            // Read the produce response
            ByteBuffer response = ByteBuffer.allocate(64);
            channel.read(response);
            response.flip();

            Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);

            if (!result.isSuccess()) {
                // If the error is "not the leader", our metadata is stale.
                // Refresh and let the caller retry.
                if (result.error.contains("Not the leader")) {
                    LOGGER.warning("Stale metadata detected — refreshing");
                    refreshMetadata();
                }
                throw new IOException("Produce failed: " + result.error);
            }

            LOGGER.info("Message sent successfully. Assigned offset: " + result.offset);
            return result.offset;
        }
    }

    // ----------------------------------------------------------------
    // Fetch (Consume)
    // ----------------------------------------------------------------

    /**
     * Fetch messages from a topic-partition starting at a given offset.
     *
     * Steps:
     *  1. Find the leader broker for this partition
     *  2. Open a connection to that broker
     *  3. Send the FETCH request with the starting offset and max bytes
     *  4. Read and decode the response
     *  5. Return the list of raw message byte arrays
     *
     * The caller is responsible for tracking the offset.
     * After processing these messages, the caller should advance
     * their offset by messages.size() before calling fetch() again.
     *
     * @param offset   the offset to start reading from
     * @param maxBytes maximum total bytes to return
     * @return list of raw message bytes (may be empty if no new messages)
     */
    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes)
            throws IOException {
        LOGGER.info("Fetching from " + topic + "-" + partition + " at offset " + offset);

        BrokerInfo leader = findLeader(topic, partition);
        if (leader == null) {
            refreshMetadata();
            leader = findLeader(topic, partition);
            if (leader == null) {
                throw new IOException(
                    "Cannot find leader for " + topic + "-" + partition
                );
            }
        }

        try (SocketChannel channel = SocketChannel.open(
                new InetSocketAddress(leader.getHost(), leader.getPort()))) {

            channel.configureBlocking(true);

            // Encode and send the fetch request
            ByteBuffer request = Protocol.encodeFetchRequest(topic, partition, offset, maxBytes);
            while (request.hasRemaining()) {
                channel.write(request);
            }

            // Read the fetch response — it may contain many messages so use a large buffer
            ByteBuffer response = ByteBuffer.allocate(maxBytes + 1024);
            channel.read(response);
            response.flip();

            Protocol.FetchResult result = Protocol.decodeFetchResponse(response);

            if (!result.isSuccess()) {
                throw new IOException("Fetch failed: " + result.error);
            }

            LOGGER.info("Fetched " + result.messages.size() + " messages");
            return result.messages != null ? result.messages : new ArrayList<>();
        }
    }

    // ----------------------------------------------------------------
    // Metadata helpers
    // ----------------------------------------------------------------

    /**
     * Look up which broker is the leader for a given topic-partition.
     *
     * Returns null if the topic or partition is not in our metadata cache.
     * Callers should refresh metadata and retry if this returns null.
     */
    private BrokerInfo findLeader(String topic, int partition) {
        TopicMetadata tm = topicMetadata.get(topic);
        if (tm == null) {
            LOGGER.warning("Topic not in metadata cache: " + topic);
            return null;
        }

        Integer leaderId = tm.getLeader(partition);
        if (leaderId == null) {
            LOGGER.warning("Partition not in metadata cache: " + topic + "-" + partition);
            return null;
        }

        BrokerInfo leader = brokerInfo.get(leaderId);
        if (leader == null) {
            LOGGER.warning("Leader broker not in metadata cache: brokerId=" + leaderId);
            return null;
        }

        return leader;
    }

    /**
     * Get the number of partitions for a topic.
     * Returns 0 if the topic is not known.
     */
    public int getPartitionCount(String topic) {
        TopicMetadata tm = topicMetadata.get(topic);
        return tm != null ? tm.getPartitionCount() : 0;
    }

    // ----------------------------------------------------------------
    // Inner classes — metadata model
    // ----------------------------------------------------------------

    /**
     * Holds metadata for a single topic:
     * which partitions exist, and who leads each one.
     *
     * This is our in-memory routing table for a topic.
     */
    public static class TopicMetadata {
        private final String topicName;

        // Maps partitionId → leaderId
        // e.g., {0 → 2, 1 → 1, 2 → 3} means partition 0 is led by broker 2, etc.
        private final Map<Integer, Integer>       partitionLeaders;

        // Maps partitionId → list of replica broker IDs (includes the leader)
        private final Map<Integer, List<Integer>> partitionReplicas;

        public TopicMetadata(String topicName) {
            this.topicName         = topicName;
            this.partitionLeaders  = new ConcurrentHashMap<>();
            this.partitionReplicas = new ConcurrentHashMap<>();
        }

        public void addPartition(int partitionId, int leaderId, List<Integer> replicaIds) {
            partitionLeaders.put(partitionId, leaderId);
            partitionReplicas.put(partitionId, replicaIds);
        }

        /**
         * Returns the broker ID of the leader for a partition,
         * or null if the partition is not known.
         */
        public Integer getLeader(int partitionId) {
            return partitionLeaders.get(partitionId);
        }

        public int getPartitionCount() {
            return partitionLeaders.size();
        }

        public String getTopicName() {
            return topicName;
        }

        @Override
        public String toString() {
            return "TopicMetadata{topic=" + topicName
                   + ", partitions=" + partitionLeaders + "}";
        }
    }
}