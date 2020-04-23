package org.blackist.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ZooWatcher implements Watcher {

    AtomicInteger seq = new AtomicInteger();
    private static final int SESSION_TIMEOUT = 10000;
    private static final String CONN_ADDR = "192.168.11.151:2181,192.168.11.152:2181,192.168.11.153:2181";
    private static final String PARENT_PATH = "/p";
    private static final String CHILD_PATH = "/p/c";
    private static final String LOG_PREFIX_OF_MAIN = "[Main]: ";
    private ZooKeeper zk = null;
    private CountDownLatch connSignal = new CountDownLatch(1);


    public void zkConnect(String hostPort, int sessionTimeout) throws Exception {
        zk = new ZooKeeper(hostPort, sessionTimeout, this);
        connSignal.await();
    }

    public void process(WatchedEvent event) {
        Event.KeeperState state = event.getState();
        Event.EventType type = event.getType();
        String path = event.getPath();
        String logPrefix = "Watcher-" + this.seq.incrementAndGet() + " ";

        System.out.println(logPrefix + "== Rcv watcher event ==");
        System.out.println(logPrefix + "Conn State: " + state.toString());
        System.out.println(logPrefix + "Event Type: " + type.toString());

        if (state == Event.KeeperState.SyncConnected) {
            processEvent(type, logPrefix);
        } else if (Event.KeeperState.Disconnected == state) {
            System.out.println("Disconnected!");
        } else if (Event.KeeperState.AuthFailed == state) {
            System.out.println("AuthFailed!");
        } else if (Event.KeeperState.Expired == state) {
            System.out.println("Session Expired!");
        }
    }

    private void processEvent(Event.EventType type, String logPrefix) {
        if (Event.EventType.None == type) {
            connSignal.countDown();
            System.out.println(logPrefix + "Connected!");
        } else if (Event.EventType.NodeCreated == type) {
            System.out.println(logPrefix + "node created");
        } else if (Event.EventType.NodeDataChanged == type) {
            System.out.println(logPrefix + "node changed");
        }
    }

    public void close() {
        try {
            if (null != zk) {
                zk.close();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean createPath(String path, String data, boolean needWatch) {
        try {
            this.zk.exists(path, needWatch);
            System.out.println(LOG_PREFIX_OF_MAIN + "node created! path: "
                    + this.zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
                    + ", content: " + data);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public String readData(String path, boolean needWatch) {
        System.out.println("Read data...");
        try {
            return new String(this.zk.getData(path, needWatch, null));
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public boolean writeData(String path, String data) {
        try {
            System.out.println(LOG_PREFIX_OF_MAIN + "data updated! path: " + path + ", stat: " + this.zk.setData(path, data.getBytes(), -1));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public void deleteNode(String path) {
        try {
            this.zk.delete(path, -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Stat exists(String path, boolean needWatch) {
        try {
            return this.zk.exists(path, needWatch);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<String> getChildren(String path, boolean needWatch) {
        System.out.println("Getting children node...");
        try {
            return this.zk.getChildren(path, needWatch);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void deleteAllTestPath(boolean needWatch) {
        if (null != this.exists(CHILD_PATH, needWatch)) {
            this.deleteNode(CHILD_PATH);
        }
        if (null != exists(PARENT_PATH, needWatch)) {
            this.deleteNode(PARENT_PATH);
        }
    }

    public static void main(String[] args) throws Exception {
        ZooWatcher zw = new ZooWatcher();
        zw.zkConnect(CONN_ADDR, SESSION_TIMEOUT);

        Thread.sleep(1000);
        // clean all nodes
        // zw.deleteAllTestPath(false);
        // create
        if (zw.createPath(PARENT_PATH, "Parent value", true)) {
            Thread.sleep(1000);
            // read data, watch again(watch has been reset when creating node)
            zw.readData(PARENT_PATH, true);
            // update data,
            zw.writeData(PARENT_PATH, "Parent new value");

            // create
            zw.createPath(CHILD_PATH, "Child value", true);
            // watch child
            System.out.println(zw.getChildren(CHILD_PATH, true));

        }

        zw.close();
    }
}
