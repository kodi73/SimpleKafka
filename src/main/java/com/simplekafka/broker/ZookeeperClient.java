import java.io.IOException;
import java.util.List;

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
}
