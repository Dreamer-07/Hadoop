package pers.prover07.zookeeper.watch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * TODO(这里用一句话描述这个类的作用)
 *
 * @author Prover07
 * @date 2023/4/25 16:40
 */
public class DistributeClient {

    private String zkServerUrl = "175.178.127.113:2181";
    private int sessionTimeout = 40000;
    private ZooKeeper zkClient;

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        DistributeClient client = new DistributeClient();

        // 初始化 zk 链接
        client.initZkConnect();
        // 获取服务列表
        client.getServerList();
        // 执行业务
        client.business();
    }

    private void initZkConnect() throws IOException {
        zkClient = new ZooKeeper(zkServerUrl, sessionTimeout, watchedEvent -> {
            try {
                // 重新注册监听
                getServerList();
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void getServerList() throws InterruptedException, KeeperException {
        List<String> servers = zkClient.getChildren("/servers", true);
        servers.forEach(server -> {
            try {
                byte[] result = zkClient.getData("/servers/" + server, false, null);
                System.out.println("server - " + server + ":" + new String(result));
            } catch (KeeperException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }


}
