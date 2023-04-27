package pers.prover07.zookeeper.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import pers.prover07.zookeeper.ZkClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * 分布式锁
 *
 * @author Prover07
 * @date 2023/4/25 17:39
 */
public class DistributedLock {

    private final String zkServerUrl = "175.178.127.113:2181";
    private final int sessionTimeout = 40000;
    private final ZooKeeper zk;

    /**
     * 等待zk链接用的
     */
    private final CountDownLatch connectLatch = new CountDownLatch(1);
    /**
     * 等待前一个锁(节点)销毁用的
     */
    private final CountDownLatch waitLatch = new CountDownLatch(1);
    /**
     * 前一个节点的路径
     */
    private String waitPath;
    /**
     * 当前创建的节点
     */
    private String currentMode;

    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        // 创建 zk 链接
        zk = new ZooKeeper(zkServerUrl, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 监听 zk 连接
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectLatch.countDown();
                }

                // 监听前一个节点的销毁事件
                if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)) {
                    waitLatch.countDown();
                }
            }
        });

        // 等待 zk 连接
        connectLatch.await();
        // 判断分布式锁节点是否存在
        Stat exists = zk.exists("/locks", false);
        // 不存在就创建
        if (exists == null) {
            zk.create("/locks", "locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    /**
     * 加锁
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void lock() throws InterruptedException, KeeperException {
        // 创建临时顺序节点(返回的节点名会带顺序)
        currentMode = zk.create("/locks/seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        // 获取所有节点
        List<String> children = zk.getChildren("/locks", null);
        // 判断是否为最小的节点
        if (children.size() == 1) {
            // 获取锁成功
            return;
        } else {
            // 判断当前节点是不是在 /locks/ 子节点中的是最小的
            String thisNode = currentMode.substring("/locks/".length());
            // children 排序
            Collections.sort(children);
            int idx = children.indexOf(thisNode);
            if (idx == -1) {
                System.out.println("数据异常，没有检查到该节点:" + thisNode);
            } else if (idx == 0) {
                // 获取锁成功
                return;
            } else {
                // 监听前一个节点
                waitPath = "/locks/" + children.get(idx - 1);
                // 监听
                zk.getData(waitPath, true, null);

                System.out.println(Thread.currentThread().getName() + "正在等待" + waitPath + "释放");

                // 等待销毁
                waitLatch.await();

                // 销毁成功，获取锁
                return;
            }
        }
    }

    /**
     * 解锁
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void unlock() throws InterruptedException, KeeperException {
        zk.delete(currentMode, -1);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributedLock lock1 = new DistributedLock();
        DistributedLock lock2 = new DistributedLock();


        new Thread(() -> {
            try {
                lock1.lock();
                System.out.println(Thread.currentThread().getName() + "获取锁成功:" + lock1.currentMode);
                Thread.sleep(5000L);
                System.out.println(Thread.currentThread().getName() + "释放锁:" + lock1.currentMode);
                lock1.unlock();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                lock2.lock();
                System.out.println(Thread.currentThread().getName() + "获取锁成功:" + lock2.currentMode);
                Thread.sleep(5000L);
                System.out.println(Thread.currentThread().getName() + "释放锁:" + lock2.currentMode);
                lock2.unlock();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            }
        }).start();

        Thread.sleep(Long.MAX_VALUE);

    }
}
