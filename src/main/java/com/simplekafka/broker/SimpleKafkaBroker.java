package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.simplekafka.broker.Protocol;

/**
 * SimpleKafkaBroker is the central server component of our Kafka-like system.
 *
 * Responsibilities:
 *  - Accept client connections (producers and consumers)
 *  - Handle produce, fetch, metadata, and create-topic requests
 *  - Register with ZooKeeper and participate in controller election
 *  - Replicate messages to follower brokers
 *  - React to cluster changes (brokers joining/leaving)
 */
public class SimpleKafkaBroker {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());

    // Where on disk we store partition log files.
    // Each broker gets its own subdirectory: /tmp/simplekafka/broker-1/
    private static final String DATA_DIR = "/tmp/simplekafka";

    // ----------------------------------------------------------------
    // Broker identity and networking
    // ----------------------------------------------------------------

    private final int    brokerId;    // Unique ID for this broker (1, 2, 3 ...)
    private final String brokerHost;  // Hostname this broker listens on
    private final int    brokerPort;  // Port this broker listens on

    // The NIO server socket — accepts incoming TCP connections
    private final ServerSocketChannel serverChannel;

    // Thread pool — each client connection is handled by a thread from here.
    // Fixed at 10 threads: enough for our demo, real Kafka uses more.
    private final ExecutorService executor;

    // ----------------------------------------------------------------
    // State flags
    // ----------------------------------------------------------------

    // AtomicBoolean is thread-safe — multiple threads can read/write safely.
    // isRunning controls the main accept loop.
    private final AtomicBoolean isRunning;

    // Whether THIS broker is the current cluster controller.
    private final AtomicBoolean isController;

    // ----------------------------------------------------------------
    // Cluster and topic metadata (in-memory)
    // ----------------------------------------------------------------

    // Maps brokerId → BrokerInfo for all known brokers in the cluster
    // ConcurrentHashMap is thread-safe — safe to read/write from multiple threads
    private final ConcurrentHashMap<Integer, BrokerInfo> clusterMetadata;

    // Maps topic name → (partition id → Partition object)
    // e.g., "payments" → {0 → Partition0, 1 → Partition1}
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Partition>> topics;

    // ----------------------------------------------------------------
    // ZooKeeper client
    // ----------------------------------------------------------------

    private final ZookeeperClient zkClient;

    // ----------------------------------------------------------------
    // Constructor
    // ----------------------------------------------------------------

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId     = brokerId;
        this.brokerHost   = host;
        this.brokerPort   = port;
        this.topics       = new ConcurrentHashMap<>();
        this.clusterMetadata = new ConcurrentHashMap<>();

        // Fixed thread pool — 10 worker threads handle client requests
        this.executor     = Executors.newFixedThreadPool(10);

        // Open the NIO server socket (not yet bound to a port)
        this.serverChannel = ServerSocketChannel.open();

        this.isRunning    = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);

        // Create per-broker data directory: /tmp/simplekafka/broker-1/
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        // ZooKeeper runs on localhost at the given port (default 2181)
        this.zkClient = new ZookeeperClient("localhost", zkPort);
    }

    // ----------------------------------------------------------------
    // Startup and Shutdown
    // ----------------------------------------------------------------

    /**
     * Start the broker:
     *  1. Bind the server socket to the port
     *  2. Register ourselves in ZooKeeper
     *  3. Try to become the controller
     *  4. Load any existing topics from ZooKeeper
     *  5. Start accepting client connections
     */
    public void start() throws IOException {
        LOGGER.info("Starting broker " + brokerId + " on " + brokerHost + ":" + brokerPort);

        // Bind and configure the NIO server socket
        // SO_REUSEADDR lets us restart quickly without "address already in use" errors
        serverChannel.socket().setReuseAddress(true);
        serverChannel.socket().bind(new InetSocketAddress(brokerHost, brokerPort));
        serverChannel.configureBlocking(false); // Non-blocking mode!

        isRunning.set(true);

        // Register with ZooKeeper so other brokers know we exist
        registerWithZookeeper();

        // Try to become the controller
        electController();

        // Load existing topics (in case we're restarting after a crash)
        loadExistingTopics();

        // Start accepting client connections in a dedicated thread
        // This thread runs acceptConnections() in a loop
        Thread acceptThread = new Thread(this::acceptConnections, "broker-" + brokerId + "-accept");
        acceptThread.setDaemon(true); // Dies when the main thread dies
        acceptThread.start();

        LOGGER.info("Broker " + brokerId + " started successfully");
    }

    /**
     * Gracefully shut down the broker.
     * Closes network connections, flushes partition files, stops threads.
     */
    public void stop() {
        LOGGER.info("Stopping broker " + brokerId);
        isRunning.set(false);

        try {
            serverChannel.close();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error closing server channel", e);
        }

        // Close all partition log files cleanly
        for (ConcurrentHashMap<Integer, Partition> topicPartitions : topics.values()) {
            for (Partition partition : topicPartitions.values()) {
                try {
                    partition.close();
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Error closing partition", e);
                }
            }
        }

        // Stop accepting new tasks and wait for running ones to finish
        executor.shutdown();

        try {
            zkClient.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        LOGGER.info("Broker " + brokerId + " stopped");
    }

    // ----------------------------------------------------------------
    // ZooKeeper Registration
    // ----------------------------------------------------------------

    /**
     * Register this broker in ZooKeeper by creating an ephemeral node.
     *
     * Path: /brokers/ids/{brokerId}
     * Data: "host:port" (so other brokers can connect to us)
     *
     * Because it's ephemeral, this node disappears automatically
     * if our ZooKeeper session dies (crash, network loss, etc.).
     * Other brokers watching /brokers/ids will be notified immediately.
     */
    private void registerWithZookeeper() {
        try {
            zkClient.connect();

            // Store our connection info so others can reach us
            String brokerData = brokerHost + ":" + brokerPort;
            String brokerPath = "/brokers/ids/" + brokerId;

            zkClient.createEphemeralNode(brokerPath, brokerData);

            // Add ourselves to local cluster metadata too
            clusterMetadata.put(brokerId, new BrokerInfo(brokerId, brokerHost, brokerPort));

            LOGGER.info("Registered broker " + brokerId + " in ZooKeeper at " + brokerPath);

            // Watch /brokers/ids so we know when brokers join or leave
            // When a broker crashes, its ephemeral node disappears,
            // triggering this callback with the updated broker list
            zkClient.watchChildren("/brokers/ids", this::onBrokersChanged);

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
            throw new RuntimeException("ZooKeeper registration failed", e);
        }
    }

    /**
     * Called by ZooKeeper when the list of brokers changes.
     *
     * This happens when:
     *  - A new broker joins (new ephemeral node appears)
     *  - A broker crashes (its ephemeral node disappears)
     *
     * We update our local cluster view and, if we're the controller,
     * trigger a rebalance to reassign any orphaned partition leaders.
     */
    private void onBrokersChanged(List<String> brokerIds) {
        LOGGER.info("Broker list changed: " + brokerIds);

        // Add any new brokers to our local metadata
        for (String brokerIdStr : brokerIds) {
            try {
                int id = Integer.parseInt(brokerIdStr);
                if (!clusterMetadata.containsKey(id)) {
                    // Fetch this broker's host:port from ZooKeeper
                    String data = zkClient.getData("/brokers/ids/" + id);
                    if (data != null) {
                        String[] parts = data.split(":");
                        clusterMetadata.put(id,
                            new BrokerInfo(id, parts[0], Integer.parseInt(parts[1])));
                        LOGGER.info("Discovered new broker: " + id + " at " + data);
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error processing broker ID: " + brokerIdStr, e);
            }
        }

        // Remove brokers that are no longer in ZooKeeper
        Set<Integer> activeIds = new HashSet<>();
        for (String id : brokerIds) {
            try { activeIds.add(Integer.parseInt(id)); }
            catch (NumberFormatException ignored) {}
        }
        clusterMetadata.keySet().retainAll(activeIds);

        // If we're the controller, rebalance partitions
        // (some might have lost their leader if a broker crashed)
        if (isController.get()) {
            rebalancePartitions();
        } else {
            // If we're not controller, maybe the controller just crashed —
            // try to elect a new one
            electController();
        }
    }

    // ----------------------------------------------------------------
    // Controller Election
    // ----------------------------------------------------------------

    /**
     * Try to become the cluster controller.
     *
     * Strategy: race to create an ephemeral node at /controller in ZooKeeper.
     * ZooKeeper guarantees only one creator succeeds (atomic operation).
     *
     * Winner → becomes controller, calls rebalancePartitions()
     * Losers → watch /controller, waiting for it to disappear
     *
     * On failure, retry after 2 seconds (ZooKeeper might be temporarily busy).
     */
    private void electController() {
        try {
            String controllerPath = "/controller";

            // Check if a controller already exists
            boolean nodeExists = zkClient.exists(controllerPath);
            if (nodeExists) {
                String existingData = zkClient.getData(controllerPath);
                // Handle stale empty nodes from previous crashes
                if (existingData == null || existingData.trim().isEmpty()) {
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                }
            }

            // Race to create the controller node
            boolean becameController = false;
            if (!nodeExists) {
                becameController = zkClient.createEphemeralNode(
                    controllerPath, String.valueOf(brokerId)
                );
            }

            if (becameController) {
                isController.set(true);
                LOGGER.info("Broker " + brokerId + " is now the CONTROLLER");
                rebalancePartitions();
            } else {
                // We didn't win — watch for when the current controller dies
                LOGGER.info("Broker " + brokerId + " is a follower. Watching controller.");
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed — retrying in 2s", e);
            // Retry after a delay — don't hammer ZooKeeper on repeated failures
            new Thread(() -> {
                try {
                    Thread.sleep(2000);
                    electController();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }

    /**
     * Called when the /controller node changes in ZooKeeper.
     * If it was deleted (controller crashed), we try to become controller.
     */
    private void onControllerChange() {
        LOGGER.info("Controller changed — attempting election");
        isController.set(false);
        electController();
    }

    // ----------------------------------------------------------------
    // Topic Management
    // ----------------------------------------------------------------

    /**
     * Load all topics that already exist in ZooKeeper.
     * Called on startup so we pick up state from before a restart.
     */
    private void loadExistingTopics() {
        try {
            List<String> topicNames = zkClient.getChildren("/topics");
            for (String topic : topicNames) {
                try {
                    loadTopic(topic);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Failed to load topic: " + topic, e);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to load existing topics", e);
        }
    }

    /**
     * Load a single topic's metadata from ZooKeeper and create
     * local Partition objects for any partitions this broker leads or follows.
     */
    private void loadTopic(String topic) throws Exception {
        if (topics.containsKey(topic)) return; // Already loaded

        // Check ZooKeeper has this topic
        if (!zkClient.exists("/topics/" + topic)) {
            LOGGER.warning("Topic " + topic + " not found in ZooKeeper");
            return;
        }

        // Create local directory structure for this topic
        String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
        new File(topicDir).mkdirs();

        ConcurrentHashMap<Integer, Partition> partitionMap = new ConcurrentHashMap<>();

        // Load each partition's state from ZooKeeper
        List<String> partitionIds = zkClient.getChildren("/topics/" + topic + "/partitions");
        for (String partIdStr : partitionIds) {
            int partId = Integer.parseInt(partIdStr);
            String statePath = "/topics/" + topic + "/partitions/" + partId + "/state";
            String stateData = zkClient.getData(statePath);

            if (stateData != null) {
                // Parse "leaderId:follower1,follower2" format
                String[] parts = stateData.split(":");
                int leaderId = Integer.parseInt(parts[0]);
                List<Integer> followerIds = new ArrayList<>();
                if (parts.length > 1 && !parts[1].isEmpty()) {
                    for (String f : parts[1].split(",")) {
                        followerIds.add(Integer.parseInt(f));
                    }
                }

                String partitionDir = topicDir + File.separator + partId;
                Partition partition = new Partition(partId, leaderId, followerIds, partitionDir);
                partitionMap.put(partId, partition);
            }
        }

        topics.put(topic, partitionMap);
        LOGGER.info("Loaded topic: " + topic + " with " + partitionMap.size() + " partitions");
    }

    /**
     * Create a new topic with the given number of partitions and replication factor.
     *
     * Only the controller broker can create topics — just like real Kafka.
     *
     * Steps:
     *  1. Create the topic directory locally
     *  2. Save topic config in ZooKeeper (persistent)
     *  3. Assign each partition a leader and followers from available brokers
     *  4. Create Partition objects locally
     *  5. Save partition state in ZooKeeper
     *  6. Notify all other brokers to load this topic
     */
    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        if (!isController.get()) {
            LOGGER.warning("Only the controller can create topics");
            return;
        }

        try {
            String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
            new File(topicDir).mkdirs();

            // Save topic-level config in ZooKeeper
            zkClient.createPersistentNode("/topics/" + topic,
                "partitions=" + numPartitions + ",replication=" + replicationFactor);
            zkClient.createPersistentNode("/topics/" + topic + "/partitions", "");

            ConcurrentHashMap<Integer, Partition> partitionMap = new ConcurrentHashMap<>();
            List<Integer> availableBrokers = new ArrayList<>(clusterMetadata.keySet());

            for (int partId = 0; partId < numPartitions; partId++) {
                // Round-robin assignment of leaders across available brokers
                // Partition 0 → broker at index 0, Partition 1 → broker at index 1, etc.
                int leaderIndex = partId % availableBrokers.size();
                int leaderId    = availableBrokers.get(leaderIndex);

                // Assign followers: the next (replicationFactor-1) brokers after the leader
                List<Integer> followerIds = new ArrayList<>();
                for (int r = 1; r < replicationFactor && r < availableBrokers.size(); r++) {
                    int followerIndex = (leaderIndex + r) % availableBrokers.size();
                    followerIds.add(availableBrokers.get(followerIndex));
                }

                // Build state string: "leaderId:follower1,follower2"
                StringBuilder state = new StringBuilder(String.valueOf(leaderId));
                if (!followerIds.isEmpty()) {
                    state.append(":");
                    for (int i = 0; i < followerIds.size(); i++) {
                        if (i > 0) state.append(",");
                        state.append(followerIds.get(i));
                    }
                }

                // Save partition state to ZooKeeper (persistent)
                zkClient.createPersistentNode(
                    "/topics/" + topic + "/partitions/" + partId, "");
                zkClient.createPersistentNode(
                    "/topics/" + topic + "/partitions/" + partId + "/state",
                    state.toString());

                // Create the Partition object locally
                String partitionDir = topicDir + File.separator + partId;
                Partition partition = new Partition(partId, leaderId, followerIds, partitionDir);
                partitionMap.put(partId, partition);

                LOGGER.info("Created partition " + partId + " for topic " + topic
                            + " — leader: " + leaderId + ", followers: " + followerIds);
            }

            topics.put(topic, partitionMap);

            // Tell all other brokers to load this topic
            notifyBrokersAboutTopic(topic);

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create topic: " + topic, e);
        }
    }

    /**
     * Send a TOPIC_NOTIFICATION to every other broker in the cluster
     * so they create their local Partition objects for the new topic.
     */
    private void notifyBrokersAboutTopic(String topic) {
        for (Map.Entry<Integer, BrokerInfo> entry : clusterMetadata.entrySet()) {
            if (entry.getKey() == brokerId) continue; // Don't notify ourselves

            BrokerInfo broker = entry.getValue();
            try (SocketChannel channel = SocketChannel.open(
                    new InetSocketAddress(broker.getHost(), broker.getPort()))) {
                channel.configureBlocking(true);
                ByteBuffer msg = Protocol.encodeTopicNotification(topic);
                while (msg.hasRemaining()) channel.write(msg);
                LOGGER.info("Notified broker " + broker.getId() + " about topic: " + topic);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING,
                    "Failed to notify broker " + broker.getId() + " about topic " + topic, e);
            }
        }
    }

    /**
     * Rebalance partition leaders across available brokers.
     *
     * Called by the controller when the cluster membership changes.
     * If a broker crashed, some partitions have no leader — we reassign them.
     */
    private void rebalancePartitions() {
        if (!isController.get()) return;

        LOGGER.info("Rebalancing partitions across " + clusterMetadata.size() + " brokers");

        List<Integer> availableBrokers = new ArrayList<>(clusterMetadata.keySet());
        if (availableBrokers.isEmpty()) return;

        for (Map.Entry<String, ConcurrentHashMap<Integer, Partition>> topicEntry : topics.entrySet()) {
            String topicName = topicEntry.getKey();
            for (Map.Entry<Integer, Partition> partEntry : topicEntry.getValue().entrySet()) {
                int       partId    = partEntry.getKey();
                Partition partition = partEntry.getValue();

                // If the current leader is no longer in the cluster, reassign
                if (!clusterMetadata.containsKey(partition.getLeader())) {
                    // Pick a new leader from available brokers
                    int newLeader = availableBrokers.get(partId % availableBrokers.size());
                    partition.setLeader(newLeader);

                    // Update ZooKeeper with new leader
                    try {
                        String statePath = "/topics/" + topicName + "/partitions/" + partId + "/state";
                        zkClient.createPersistentNode(statePath, String.valueOf(newLeader));
                        LOGGER.info("Reassigned leader for " + topicName
                                    + "-" + partId + " to broker " + newLeader);
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Failed to update partition state in ZooKeeper", e);
                    }
                }
            }
        }
    }

    // ----------------------------------------------------------------
    // Network: Accept Connections
    // ----------------------------------------------------------------

    /**
     * Main accept loop — runs in its own thread.
     *
     * ServerSocketChannel is non-blocking: accept() returns null
     * immediately if no client is waiting (instead of blocking).
     * We add a small sleep to avoid busy-spinning the CPU.
     */
    private void acceptConnections() {
        LOGGER.info("Broker " + brokerId + " accepting connections on port " + brokerPort);
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    // A client connected! Handle it in a thread pool thread
                    // so we can immediately go back to accepting more connections
                    LOGGER.info("New connection from: " + clientChannel.getRemoteAddress());
                    executor.submit(() -> handleClient(clientChannel));
                }
                Thread.sleep(10); // 10ms pause prevents CPU spinning at 100%
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }

    /**
     * Handle a single client connection.
     *
     * Reads requests in a loop until the client disconnects.
     * Each request starts with a type byte, then the payload.
     */
    private void handleClient(SocketChannel clientChannel) {
        try {
            // Allocate a 64KB buffer — large enough for most requests
            ByteBuffer buffer = ByteBuffer.allocate(65536);

            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();

                // configureBlocking(true) here so read() waits for data
                // (the channel was non-blocking for accept, but we want
                // blocking reads when processing a specific client)
                clientChannel.configureBlocking(true);
                int bytesRead = clientChannel.read(buffer);

                if (bytesRead > 0) {
                    buffer.flip(); // Switch from write mode to read mode
                    processClientMessage(clientChannel, buffer);
                } else if (bytesRead < 0) {
                    // -1 means client closed the connection cleanly
                    LOGGER.info("Client disconnected: " + clientChannel.getRemoteAddress());
                    clientChannel.close();
                    break;
                }

            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error handling client", e);
        } finally {
            try {
                if (clientChannel.isOpen()) clientChannel.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing client channel", e);
            }
        }
    }

    /**
     * Read the first byte of a request to determine its type,
     * then dispatch to the appropriate handler.
     *
     * This is the protocol dispatcher — the first byte is always
     * the message type constant defined in Protocol.java.
     */
    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {
        byte messageType = buffer.get(); // Read the 1-byte type prefix

        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type: " + messageType);
        }
    }

    // ----------------------------------------------------------------
    // Request Handlers
    // ----------------------------------------------------------------

    /**
     * Handle a PRODUCE request from a producer client.
     *
     * Decodes: [topicLen(2)][topic(N)][partition(4)][msgLen(4)][message(N)]
     *
     * If this broker is the leader for the partition:
     *   → append to local log
     *   → replicate to followers
     *   → respond with the assigned offset
     *
     * If NOT the leader:
     *   → respond with an error (client should redirect to leader)
     */
    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {
        // Decode the topic name
        short topicLen  = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic    = new String(topicBytes);

        int partitionId = buffer.getInt();

        // Decode the message
        int msgLen      = buffer.getInt();
        byte[] message  = new byte[msgLen];
        buffer.get(message);

        LOGGER.info("PRODUCE request: topic=" + topic
                    + " partition=" + partitionId + " msgLen=" + msgLen);

        // Verify the topic and partition exist
        if (!topics.containsKey(topic) || !topics.get(topic).containsKey(partitionId)) {
            Protocol.sendErrorResponse(clientChannel,
                "Unknown topic/partition: " + topic + "-" + partitionId);
            return;
        }

        Partition partition = topics.get(topic).get(partitionId);

        // Only the leader can accept writes
        if (partition.getLeader() != brokerId) {
            Protocol.sendErrorResponse(clientChannel,
                "Not the leader for " + topic + "-" + partitionId
                + ". Leader is broker " + partition.getLeader());
            return;
        }

        // Append to our local log and get back the assigned offset
        long offset = partition.append(message);

        // Replicate to all follower brokers (in background threads)
        replicateToFollowers(topic, partition, message, offset);

        // Send success response: [PRODUCE_RESPONSE(1)][offset(8)][status(1)]
        ByteBuffer response = ByteBuffer.allocate(1 + 8 + 1);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.put((byte) 0); // 0 = success
        response.flip();
        while (response.hasRemaining()) clientChannel.write(response);
    }

    /**
     * Handle a FETCH request from a consumer client.
     *
     * Decodes: [topicLen(2)][topic(N)][partition(4)][offset(8)][maxBytes(4)]
     *
     * Reads messages from the partition log starting at the given offset
     * and sends them back.
     */
    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {
        short topicLen  = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic    = new String(topicBytes);

        int  partitionId = buffer.getInt();
        long offset      = buffer.getLong();
        int  maxBytes    = buffer.getInt();

        LOGGER.info("FETCH request: topic=" + topic
                    + " partition=" + partitionId + " offset=" + offset);

        if (!topics.containsKey(topic) || !topics.get(topic).containsKey(partitionId)) {
            Protocol.sendErrorResponse(clientChannel,
                "Unknown topic/partition: " + topic + "-" + partitionId);
            return;
        }

        Partition partition = topics.get(topic).get(partitionId);

        // Check if there are any messages at the requested offset
        if (offset >= partition.getNextOffset()) {
            // No messages yet at this offset — send empty response
            ByteBuffer response = ByteBuffer.allocate(1 + 4);
            response.put(Protocol.FETCH_RESPONSE);
            response.putInt(0); // 0 messages
            response.flip();
            while (response.hasRemaining()) clientChannel.write(response);
            return;
        }

        // Read messages from the partition log
        List<byte[]> messages = partition.readMessages(offset, maxBytes);

        // Build the response:
        // [FETCH_RESPONSE(1)][msgCount(4)] then for each message:
        // [offset(8)][msgLen(4)][msgData(N)]
        int responseSize = 1 + 4; // type + count
        for (byte[] msg : messages) responseSize += 8 + 4 + msg.length;

        ByteBuffer response = ByteBuffer.allocate(responseSize);
        response.put(Protocol.FETCH_RESPONSE);
        response.putInt(messages.size());

        long currentOffset = offset;
        for (byte[] msg : messages) {
            response.putLong(currentOffset++); // offset of this specific message
            response.putInt(msg.length);
            response.put(msg);
        }
        response.flip();
        while (response.hasRemaining()) clientChannel.write(response);
    }

    /**
     * Handle a METADATA request.
     *
     * Returns: all broker connection info + all topic/partition assignments.
     * Clients use this to discover which broker leads which partition.
     */
    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {
        LOGGER.info("METADATA request from client");

        // Calculate response size
        // Brokers: [count(4)] + per broker: [id(4)][hostLen(2)][host(N)][port(4)]
        // Topics:  [count(4)] + per topic: [topicLen(2)][topic(N)][partCount(4)]
        //          + per partition: [id(4)][leaderId(4)][replicaCount(4)][replicaIds(4*N)]

        int size = 1 + 4; // type + broker count
        for (BrokerInfo b : clusterMetadata.values()) {
            size += 4 + 2 + b.getHost().getBytes().length + 4;
        }
        size += 4; // topic count
        for (Map.Entry<String, ConcurrentHashMap<Integer, Partition>> te : topics.entrySet()) {
            size += 2 + te.getKey().getBytes().length + 4;
            for (Partition p : te.getValue().values()) {
                size += 4 + 4 + 4 + (p.getFollowers().size() * 4);
            }
        }

        ByteBuffer response = ByteBuffer.allocate(size);
        response.put(Protocol.METADATA_RESPONSE);

        // Write broker info
        response.putInt(clusterMetadata.size());
        for (BrokerInfo b : clusterMetadata.values()) {
            byte[] hostBytes = b.getHost().getBytes();
            response.putInt(b.getId());
            response.putShort((short) hostBytes.length);
            response.put(hostBytes);
            response.putInt(b.getPort());
        }

        // Write topic info
        response.putInt(topics.size());
        for (Map.Entry<String, ConcurrentHashMap<Integer, Partition>> te : topics.entrySet()) {
            byte[] topicBytes = te.getKey().getBytes();
            response.putShort((short) topicBytes.length);
            response.put(topicBytes);
            response.putInt(te.getValue().size());
            for (Partition p : te.getValue().values()) {
                response.putInt(p.getId());
                response.putInt(p.getLeader());
                response.putInt(p.getFollowers().size());
                for (int replicaId : p.getFollowers()) response.putInt(replicaId);
            }
        }

        response.flip();
        while (response.hasRemaining()) clientChannel.write(response);
    }

    /**
     * Handle a CREATE_TOPIC request.
     *
     * Decodes: [topicLen(2)][topic(N)][numPartitions(4)][replicationFactor(2)]
     *
     * Forwards to createTopic() which only executes if this broker
     * is the controller.
     */
    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {
        short topicLen  = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic    = new String(topicBytes);

        int   numPartitions     = buffer.getInt();
        short replicationFactor = buffer.getShort();

        LOGGER.info("CREATE_TOPIC request: topic=" + topic
                    + " partitions=" + numPartitions
                    + " replication=" + replicationFactor);

        createTopic(topic, numPartitions, replicationFactor);

        // Send a simple acknowledgement
        ByteBuffer response = ByteBuffer.allocate(1 + 8 + 1);
        response.put(Protocol.PRODUCE_RESPONSE); // Reuse produce response format
        response.putLong(0);
        response.put((byte) 0); // 0 = success
        response.flip();
        while (response.hasRemaining()) clientChannel.write(response);
    }

    /**
     * Handle a REPLICATE request from a leader broker.
     *
     * The leader has already appended the message to its own log.
     * Now it's telling us (a follower) to do the same.
     *
     * Decodes: [topicLen(2)][topic(N)][partition(4)][offset(8)][msgLen(4)][msg(N)]
     */
    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {
        short topicLen  = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic    = new String(topicBytes);

        int  partitionId = buffer.getInt();
        long offset      = buffer.getLong(); // The offset the leader assigned
        int  msgLen      = buffer.getInt();
        byte[] message   = new byte[msgLen];
        buffer.get(message);

        LOGGER.info("REPLICATE request: topic=" + topic
                    + " partition=" + partitionId + " offset=" + offset);

        // Make sure we have this topic/partition locally
        if (!topics.containsKey(topic)) {
            try { loadTopic(topic); } catch (Exception e) {
                Protocol.sendErrorResponse(clientChannel, "Cannot load topic: " + topic);
                return;
            }
        }

        if (!topics.containsKey(topic) || !topics.get(topic).containsKey(partitionId)) {
            Protocol.sendErrorResponse(clientChannel,
                "Unknown topic/partition for replication: " + topic + "-" + partitionId);
            return;
        }

        // Append the message to our local replica
        Partition partition = topics.get(topic).get(partitionId);
        partition.append(message);

        // Send acknowledgement back to the leader
        ByteBuffer response = ByteBuffer.allocate(1 + 8 + 1);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.put((byte) 0); // 0 = success
        response.flip();
        while (response.hasRemaining()) clientChannel.write(response);
    }

    /**
     * Handle a TOPIC_NOTIFICATION from the controller.
     *
     * The controller created a new topic and is telling us to load it
     * from ZooKeeper so we have the partition assignments locally.
     */
    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {
        short topicLen  = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic    = new String(topicBytes);

        LOGGER.info("TOPIC_NOTIFICATION: loading topic " + topic);

        try {
            loadTopic(topic);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to load topic from notification: " + topic, e);
        }
    }

    /**
     * Replicate a message to all follower brokers for a partition.
     *
     * Each follower gets its own thread so replication is parallel —
     * a slow follower doesn't block other followers or the producer response.
     *
     * In real Kafka, the leader waits for a configurable number of
     * acknowledgements (acks) before confirming to the producer.
     * We send fire-and-forget for simplicity.
     */
    private void replicateToFollowers(String topic, Partition partition,
                                      byte[] message, long offset) {
        for (int followerId : partition.getFollowers()) {
            BrokerInfo follower = clusterMetadata.get(followerId);
            if (follower == null) continue; // Follower no longer in cluster

            // Each replication runs in its own thread
            executor.submit(() -> {
                try (SocketChannel channel = SocketChannel.open(
                        new InetSocketAddress(follower.getHost(), follower.getPort()))) {

                    channel.configureBlocking(true);

                    ByteBuffer replicateMsg = Protocol.encodeReplicateRequest(
                        topic, partition.getId(), offset, message
                    );
                    while (replicateMsg.hasRemaining()) channel.write(replicateMsg);

                    // Read acknowledgement from follower
                    ByteBuffer ack = ByteBuffer.allocate(10);
                    channel.read(ack);

                    LOGGER.info("Replicated offset " + offset + " to broker " + followerId);

                } catch (IOException e) {
                    LOGGER.log(Level.WARNING,
                        "Failed to replicate to broker " + followerId, e);
                }
            });
        }
    }

    // ----------------------------------------------------------------
    // Main entry point
    // ----------------------------------------------------------------

    /**
     * Start a broker from the command line.
     *
     * Usage: SimpleKafkaBroker <brokerId> <host> <port> <zkPort>
     *
     * Example: SimpleKafkaBroker 1 localhost 9091 2181
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: SimpleKafkaBroker <brokerId> <host> <port> <zkPort>");
            System.exit(1);
        }

        int    brokerId = Integer.parseInt(args[0]);
        String host     = args[1];
        int    port     = Integer.parseInt(args[2]);
        int    zkPort   = Integer.parseInt(args[3]);

        SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, host, port, zkPort);
        broker.start();

        // Add a shutdown hook so stop() is called when Ctrl+C is pressed
        Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));

        // Keep the main thread alive — the broker runs in background threads
        Thread.currentThread().join();
    }
}