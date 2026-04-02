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

    
}
