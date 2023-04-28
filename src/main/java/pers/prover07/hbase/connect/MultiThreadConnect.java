package pers.prover07.hbase.connect;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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

    /**
     * 判断表格是否存在
     */
    @Test
    public void isTableExists() throws IOException {
        // admin 的连接是轻量级的 不是线程安全的 不推荐池化或者缓存这个连接
        Admin admin = connection.getAdmin();

        boolean status = admin.tableExists(TableName.valueOf("bigData", "test1"));

        admin.close();

        System.out.println("status:" + status);
    }

    /**
     * 创建表
     */
    @Test
    public void createTable() {
        try (Admin admin = connection.getAdmin();) {
            // 描述表信息
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf("bigData", "test1"));

            // 定义列族信息
            String[] columns = new String[]{"info", "options"};
            for (String column : columns) {
                ColumnFamilyDescriptorBuilder cfdBuilder = ColumnFamilyDescriptorBuilder.newBuilder(column.getBytes());
                cfdBuilder.setMaxVersions(3);
                tableDescriptorBuilder.setColumnFamily(cfdBuilder.build());
            }

            // 创建表格
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 修改表(修改/添加列族)
     */
    @Test
    public void alterTable() {
        try (Admin admin = connection.getAdmin()) {
            // 获取表的描述信息
            TableName tableName = TableName.valueOf("bigData", "test1");

            // 获取表的描述器
            TableDescriptor tableDescriptor = admin.getDescriptor(tableName);

            String[] columns = new String[]{"info", "test2"};

            for (String column : columns) {
                // 判断改列族是否存在
                boolean hasColumnFamily = tableDescriptor.hasColumnFamily(column.getBytes());

                if (hasColumnFamily) {
                    // 修改
                    ColumnFamilyDescriptor familyDescriptor = tableDescriptor.getColumnFamily(column.getBytes());
                    // familyDescriptor.setMax
                    familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(familyDescriptor)
                            .setMaxVersions(5).build();
                    admin.modifyColumnFamily(tableName, familyDescriptor);
                } else {
                    // 添加
                    ColumnFamilyDescriptorBuilder familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(column.getBytes());
                    familyDescriptor.setMaxVersions(3);
                    admin.addColumnFamily(tableName, familyDescriptor.build());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除列族
     */
    @Test
    public void deleteTableColumn() {

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
