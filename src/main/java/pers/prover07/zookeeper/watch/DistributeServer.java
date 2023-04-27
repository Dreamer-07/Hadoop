package pers.prover07.zookeeper.watch;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * 服务端
 *
 * @author Prover07
 * @date 2023/4/25 16:33
 */
public class DistributeServer {

    private String zkServerUrl = "175.178.127.113:2181";
    private int sessionTimeout = 40000;
    private ZooKeeper zkClient;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeServer distributeServer = new DistributeServer();
        // 注册到 zk
        distributeServer.getZkConnect();
        // 创建节点
        distributeServer.registerNode("server-5");
        // 执行业务
        distributeServer.start();
    }

    private void getZkConnect() throws IOException {
        zkClient = new ZooKeeper(zkServerUrl, sessionTimeout, watchedEvent -> {

        });
    }

    private void registerNode(String hostname) throws InterruptedException, KeeperException {
        zkClient.create("/servers/" + hostname, hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    private void start() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }


}
