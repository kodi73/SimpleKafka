import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

public class ZookeeperClient {
    public void connect() throws IOException, InterruptedException {
        zookeeper = new Zookeeper(getConnectString(), SESSION_TIMOUT, this);
        connectedSignal.await();

        createPath("/brokers");
        createPath("/topics");
        createPath("/controller");
    }

    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zookeeper.exists(path, false);

        if (stat == null) {
            zookeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } else {
            zookeeper.setData(path, daata.getBytes(), -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zookeeper.exists(path, false);

        if (stat == null) {
            zookeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node:" + path);
            return true;
        } else {
            LOGGER.info("Ephemeral node already exists: " + path);
            return false;
        }
    }

    /**
     * This function is used for detecting 
     *      watches new brokers
     *      watches new topics
     *      tracks consumer group memberships
     * @param path
     * @param callback
     */
    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        List<String> newChildren = zooKeeper.getChildren(path, event2 -> {
                            if (event2.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                                watchChildren(path, callback);
                            }
                        });
                        callback.onChildrenChanged(newChildren);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error processing children changed event", e);
                    }
                }
            });
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
        }
    }

    /**
     * This function
     *      watches controller changes
     *      watch partition leader elections
     *      track changes in topic configurations
     * @param path
     * @param callback
     */

    public void watchNode(String path, NodeCallback callback) {
        try {
            zookeeper.exists(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted ||
                    event.getType() == Watcher.Event.EventType.NodeDataChanged || 
                    event.getType() == Watcher.Event.EventType.NodeCreated) {
                        callback.onNodeChanged();
                    }
            });
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }

    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            LOGGER.info("Connected to Zookeeper");
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            LOGGER.warning("Disconnected from Zookeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            LOGGER.warning("ZooKeeper session expired, reconnecting...");
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }
}
