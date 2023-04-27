package pers.prover07.zookeeper;

import io.netty.channel.ChannelId;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * TODO(这里用一句话描述这个类的作用)
 *
 * @author Prover07
 * @date 2023/4/25 9:41
 */
public class ZkClient {

    private String zkServerUrl = "175.178.127.113:2181";
    private int sessionTimeout = 40000;

    private ZooKeeper client;

    @Before
    public void initConnect() throws IOException {
        client = new ZooKeeper(zkServerUrl, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    /**
     * 创建子节点
     */
    @Test
    public void createNode() throws InterruptedException, KeeperException {
        /*
        * 节点路径
        * 节点参数
        * 节点权限
        * 节点类型:
        *  PERSISTENT:普通持久几点
        *  PERSISTENT_SEQUENTIAL：顺序持久节点
        *  EPHEMERAL：普通临时节点
        *  EPHEMERAL_SEQUENTIAL：顺序临时节点
        * */
        client.create("/B", "abc".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void existsNode() throws InterruptedException, KeeperException {
        Stat existsStatus = client.exists("/B", false);
        if (existsStatus != null) {
            System.out.println("/B exists");
        } else {
            System.out.println("/B not exists");
        }
    }

    @Test
    public void getData() throws InterruptedException, KeeperException {
        byte[] data = client.getData("/B", null, null);
        String result = new String(data);
        System.out.println("get data:" + result);
    }

    @Test
    public void getStat() throws InterruptedException, KeeperException {
        Stat stat = new Stat();
        byte[] data = client.getData("/B", null, stat);
        String result = new String(data);
        System.out.println("get data:" + result);
        System.out.println("get stat:" + stat);
    }

    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        List<String> children = client.getChildren("/", true);
        children.forEach(System.out::println);
    }

    @Test
    public void getChildrenData() throws InterruptedException, KeeperException {
        List<String> children = client.getChildren("/", true);
        for (String child : children) {
            byte[] data = client.getData("/" + child, null, null);
            System.out.println(child + ":" + new String(data));
        }
    }

    @Test
    public void updateNodeInfo() throws InterruptedException, KeeperException {
        // -1 表示无论版本是多少都可以修改
        Stat stat = client.setData("/B", "byq".getBytes(), -1);
        System.out.println("get stat:" + stat);
    }

    @Test
    public void deleteNodeinfo() throws InterruptedException, KeeperException {
        client.delete("/B", -1);
    }

    @Test
    public void watchNodeInfo() throws InterruptedException, KeeperException {
        byte[] data = client.getData("/B", watchedEvent -> {
            System.out.println("getDataWatcher event type:{" + watchedEvent.getType() + "}");
            System.out.println(watchedEvent);
        }, null);

        String result = new String(data);
        System.out.println("get data:" + result);

        // 修改节点值
        Stat stat = client.setData("/B", "xixi".getBytes(), -1);

        Thread.sleep(30000);
    }

}
