package pers.prover07.hbase.connect;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;

/**
 * TODO(这里用一句话描述这个类的作用)
 *
 * @author Prover07
 * @date 2023/4/27 14:45
 */
public class MultiThreadConnect {

    /**
     * 设置静态属性，避免反复创建
     */
    public static Connection connection = null;

    static {
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 创建命名空间(数据库)
     */
    @Test
    public void createNamespace() throws IOException {
        // admin 的连接是轻量级的 不是线程安全的 不推荐池化或者缓存这个连接
        Admin admin = connection.getAdmin();

        // 配置命名空间
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("bigData");
        // 添加属性
        builder.addConfiguration("user", "byq");

        // 创建命名空间
        admin.createNamespace(builder.build());

        // 关闭 admin
        admin.close();
    }

    public static void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
