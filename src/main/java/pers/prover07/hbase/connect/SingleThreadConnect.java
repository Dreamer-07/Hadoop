package pers.prover07.hbase.connect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * TODO(这里用一句话描述这个类的作用)
 *
 * @author Prover07
 * @date 2023/4/27 11:12
 */
public class SingleThreadConnect {

    public static void main(String[] args) throws IOException {
        // 配置连接参数
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "175.178.127.113");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.session.timeout", "4000");
        // 创建连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        System.out.println(connection);
        // 关闭连接
        connection.close();
    }

}
