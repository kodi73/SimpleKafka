package com.simplekafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ZookeeperClient wraps Apache ZooKeeper to provide the coordination
 * services that SimpleKafka needs:
 *   - broker registration (ephemeral nodes)
 *   - topic/partition metadata storage (persistent nodes)
 *   - controller election (first to create /controller wins)
 *   - change notifications (watches on broker list, controller node)
 */
public class ZookeeperClient implements Watcher {

    private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());

    // How long ZooKeeper waits before declaring a session dead (ms).
    // If a broker freezes for longer than this, its ephemeral nodes vanish.
    private static final int SESSION_TIMEOUT = 10000;

    private final String host;
    private final int port;
    private ZooKeeper zooKeeper;

    // CountDownLatch is a one-time gate. We initialise it at 1,
    // then call countDown() when ZooKeeper confirms connection.
    // connect() blocks on await() until that happens.
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    // ----------------------------------------------------------------
    // Callback interfaces — callers register these to react to changes
    // ----------------------------------------------------------------

    /**
     * Called when the list of child nodes under a path changes.
     * e.g., a new broker joins → /brokers/ids gets a new child.
     */
    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    /**
     * Called when a specific node is created, deleted, or its data changes.
     * e.g., the /controller node disappears → trigger a new election.
     */
    public interface NodeCallback {
        void onNodeChanged();
    }

    public ZookeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    // ----------------------------------------------------------------
    // CONNECTION MANAGEMENT
    // ----------------------------------------------------------------

    /**
     * Connect to ZooKeeper and create the base directory structure.
     *
     * We pass 'this' as the Watcher — meaning this class receives
     * all session-level events (connected, disconnected, expired).
     *
     * We then block on connectedSignal.await() — we don't proceed
     * until ZooKeeper confirms the session is live.
     */
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await(); // Block until process() signals SyncConnected

        // Create the root paths all brokers need.
        // These are persistent — they survive broker restarts.
        createPath("/brokers");
        createPath("/brokers/ids");
        createPath("/topics");
        createPath("/controller");
    }

    /**
     * This is the main ZooKeeper event handler (from the Watcher interface).
     * ZooKeeper calls this on a background thread whenever a session
     * state change happens.
     */
    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            // Connection established — release the await() in connect()
            connectedSignal.countDown();
            LOGGER.info("Connected to ZooKeeper");

        } else if (event.getState() == Event.KeeperState.Disconnected) {
            // Network blip — ZooKeeper will try to reconnect automatically
            // within the session timeout window. We just log it.
            LOGGER.warning("Disconnected from ZooKeeper");

        } else if (event.getState() == Event.KeeperState.Expired) {
            // The session expired — all our ephemeral nodes are GONE.
            // This means other brokers now think we're dead.
            // We must reconnect and re-register from scratch.
            LOGGER.warning("ZooKeeper session expired — reconnecting...");
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
                // Reset the latch for the new connection attempt
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }

    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    private String getConnectString() {
        return host + ":" + port;
    }

    // ----------------------------------------------------------------
    // NODE CREATION
    // ----------------------------------------------------------------

    /**
     * Create a path (like a directory) if it doesn't exist yet.
     * Used for the base structure: /brokers, /topics, /controller.
     * These are persistent and hold no meaningful data themselves.
     */
    private void createPath(String path) {
        try {
            Stat stat = zooKeeper.exists(path, false);
            if (stat == null) {
                zooKeeper.create(path, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            // Another broker may have created it simultaneously — that's fine
            LOGGER.log(Level.WARNING, "Could not create path: " + path, e);
        }
    }

    /**
     * Create or update a PERSISTENT node.
     *
     * Persistent nodes survive session expiry and broker restarts.
     * Used for: topic configs, partition state (leader/replicas).
     *
     * If the node already exists, we update its data.
     * If it doesn't, we create it fresh.
     */
    public void createPersistentNode(String path, String data)
            throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            // Node doesn't exist — create it
            // We also make sure the parent path exists first
            ensureParentExists(path);
            zooKeeper.create(path, data.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } else {
            // Node exists — just update its data
            // -1 means "match any version" (no optimistic locking)
            zooKeeper.setData(path, data.getBytes(), -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    /**
     * Create an EPHEMERAL node.
     *
     * Ephemeral nodes vanish when this ZooKeeper session ends —
     * whether due to a clean shutdown or a crash.
     *
     * Used for: broker registration (/brokers/ids/1) and controller
     * election (/controller).
     *
     * Returns true if we successfully created the node,
     * false if it already existed (someone else got there first).
     */
    public boolean createEphemeralNode(String path, String data)
            throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            ensureParentExists(path);
            zooKeeper.create(path, data.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node: " + path);
            return true; // We won the race
        } else {
            LOGGER.info("Ephemeral node already exists: " + path);
            return false; // Someone else already holds this node
        }
    }

    // ----------------------------------------------------------------
    // DATA ACCESS
    // ----------------------------------------------------------------

    /**
     * Read the data stored at a ZooKeeper node.
     * Returns null if the node doesn't exist.
     */
    public String getData(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) return null;
        byte[] data = zooKeeper.getData(path, false, null);
        return data != null ? new String(data) : null;
    }

    /**
     * Check if a node exists.
     */
    public boolean exists(String path) throws KeeperException, InterruptedException {
        return zooKeeper.exists(path, false) != null;
    }

    /**
     * Delete a node (used when cleaning up a stale /controller node).
     */
    public void deleteNode(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat != null) {
            zooKeeper.delete(path, -1);
        }
    }

    /**
     * Get the list of children under a path without setting a watch.
     * Returns an empty list if the path doesn't exist.
     */
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) return new ArrayList<>();
        return zooKeeper.getChildren(path, false);
    }

    // ----------------------------------------------------------------
    // WATCHES — React to cluster changes without polling
    // ----------------------------------------------------------------

    /**
     * Watch the children of a path.
     *
     * When a child is added or removed (e.g., a broker joins or crashes),
     * ZooKeeper fires a NodeChildrenChanged event. We then:
     *   1. Fetch the new child list
     *   2. Re-register the watch (watches are one-shot in ZooKeeper!)
     *   3. Call the callback so the broker can react
     *
     * This is how we detect: new broker joined, broker crashed.
     */
    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        // Re-register watch and fetch new children
                        List<String> newChildren = zooKeeper.getChildren(path, event2 -> {
                            if (event2.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                                watchChildren(path, callback); // Keep watching recursively
                            }
                        });
                        callback.onChildrenChanged(newChildren);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error processing children changed event", e);
                    }
                }
            });
            // Immediately call back with the current list
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
        }
    }

    /**
     * Watch a single node for any change (created, deleted, data changed).
     *
     * Used to watch the /controller node — if it disappears,
     * all brokers race to become the new controller.
     *
     * Again, watches are one-shot, so we re-register inside the handler.
     */
    public void watchNode(String path, NodeCallback callback) {
        try {
            zooKeeper.exists(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted ||
                    event.getType() == Watcher.Event.EventType.NodeDataChanged ||
                    event.getType() == Watcher.Event.EventType.NodeCreated) {
                    callback.onNodeChanged();
                    // Re-register the watch so we keep getting notified
                    watchNode(path, callback);
                }
            });
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }

    // ----------------------------------------------------------------
    // HELPER
    // ----------------------------------------------------------------
    private void ensureParentExists(String path) {
        String[] parts = path.split("/");
        StringBuilder current = new StringBuilder();
        for (int i = 1; i < parts.length - 1; i++) {
            current.append("/").append(parts[i]);
            createPath(current.toString());
        }
    }
}