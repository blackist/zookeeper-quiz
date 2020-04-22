package org.blackist.zookeeper;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Zoo {

    private ZooKeeper zk;
    private CountDownLatch connSignal = new CountDownLatch(1);

    public ZooKeeper zkConnect(String hostPort) throws Exception {
        zk = new ZooKeeper(hostPort, 3000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connSignal.countDown();
                    System.out.println("Connected!");
                }
            }
        });
        connSignal.await();
        return zk;
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public static void main(String[] args) throws Exception {
        Zoo zoo = new Zoo();
        ZooKeeper zk = zoo.zkConnect("192.168.11.151:2181");

        String newNode = "/blackist-20200422";

        // create if not exist
        if (zk.exists(newNode, true) == null) {
            // sync creation
            // zk.create(newNode, "2020-04-22".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            // async creation
            zk.create(newNode, "2020-04-22".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
                public void processResult(int rc, String path, Object ctx, String name) {
                    System.out.println("rc: " + rc);
                    System.out.println("path: " + path);
                    System.out.println("ctx: " + ctx);
                    System.out.println("name: " + name);
                }
            }, "param");
        }

        // read all nodes
        List<String> zNodes = zk.getChildren("/", true);
        System.out.println("ChildrenNodes: " + zNodes);

        // sleep for async creation
        TimeUnit.SECONDS.sleep(5);

        // get node value
        // byte[] data = zk.getData(newNode, true, zk.exists(newNode, true));
        // System.out.println("GetData before setting: ");
        // for (byte dataPoint : data) {
        //     System.out.print((char) dataPoint);
        // }
        // System.out.println();

        // set node value
        // System.out.println("GetData after setting: ");
        // zk.setData(newNode, ("Modified " + new Date()).getBytes(), zk.exists(newNode, true).getVersion());
        // data = zk.getData(newNode, true, zk.exists(newNode, true));
        // for (byte dataPoint : data) {
        //     System.out.print((char) dataPoint);
        // }
        // System.out.println();

        // delete node
        zk.delete(newNode, zk.exists(newNode, true).getVersion());
        zoo.close();
    }
}
