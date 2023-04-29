package pers.prover07.hbase.connect;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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
        try (Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf("bigData", "test1");
            admin.deleteColumnFamily(tableName, "test2".getBytes());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 插入数据
     */
    @Test
    public void insertData() {
        // 获取 table 连接
        try (Table table = connection.getTable(TableName.valueOf("bigData", "test1"))) {
            // 创建写入数据对象(RowKey)
            Put put = new Put("1001".getBytes());
            // 设置列族，列名，数据
            put.addColumn("info".getBytes(), "name".getBytes(), "prover2".getBytes());
            put.addColumn("options".getBytes(), "disabled".getBytes(), "0".getBytes());

            // 插入数据
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取数据(读取对应一行的某一列)
     */
    @Test
    public void getData() {
        // 获取表信息
        try (Table table = connection.getTable(TableName.valueOf("bigData", "test1"))) {
            // 创建 get 对象(RowKey)
            Get get = new Get("1001".getBytes());

            // 配置要读取的列
            get.addColumn("info".getBytes(), "name".getBytes());
            get.addColumn("options".getBytes(), "disabled".getBytes());

            // 设置要读取的数据版本
            get.readAllVersions();

            // 获取数据
            Result result = table.get(get);

            // 处理数据
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(new String(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取数据(读取多行)
     */
    @Test
    public void scanRows() {
        try (// 获取 table
             Table table = connection.getTable(TableName.valueOf("bigData", "test1"));
        ) {

            // 创建 scan
            Scan scan = new Scan();
            // 配置 scan(不配置会扫描所有数据)
            // 默认包含
            scan.withStartRow("1001".getBytes());
            // 默认包含
            scan.withStopRow("1010".getBytes());

            // 读取数据
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell)) + "-" +
                            new String(CellUtil.cloneFamily(cell)) + "-" +
                            new String(CellUtil.cloneQualifier(cell)) + "-" +
                            new String(CellUtil.cloneValue(cell)) + "\t"
                    );
                }
                System.out.println();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取数据(带过滤条件)
     */
    @Test
    public void filterScan() {
        try (// 获取 table
             Table table = connection.getTable(TableName.valueOf("bigData", "test1"))
        ) {
            // 创建 scan
            Scan scan = new Scan();
            // 配置 scan(不配置会扫描所有数据)
            // 默认包含
            scan.withStartRow("1001".getBytes());
            // 默认包含
            scan.withStopRow("1010".getBytes());

            // 配置过滤条件
            FilterList filterList = new FilterList();
            // 创建过滤器
            // 1. ColumnValueFilter - 只显示配置的列信息
            // ColumnValueFilter filter = new ColumnValueFilter("info".getBytes(), "name".getBytes(), CompareOperator.EQUAL, "prover2".getBytes());

            // 2. SingleColumnValueFilter - 会显示符合条件(包括没有该列)的所有列信息
            // 例如 1002 没有 info.name 这个列的数据，那么也会显示
            // 但 1003 有 info.name 的数据，但是不符合条件, 就不会显示
            // 且除了 name 列，符合条件的行的所有列都会显示出来
            SingleColumnValueFilter filter = new SingleColumnValueFilter("info".getBytes(), "name".getBytes(), CompareOperator.EQUAL, "prover".getBytes());

            // 添加到过滤器链中
            filterList.addFilter(filter);
            scan.setFilter(filterList);

            // 读取数据
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell)) + "-" +
                            new String(CellUtil.cloneFamily(cell)) + "-" +
                            new String(CellUtil.cloneQualifier(cell)) + "-" +
                            new String(CellUtil.cloneValue(cell)) + "\t"
                    );
                }
                System.out.println();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除数据
     */
    @Test
    public void deleteData() {
        try (// 获取 table
             Table table = connection.getTable(TableName.valueOf("bigData", "test1"))
        ) {
            // 创建 delete 对象(RowKey)
            Delete delete = new Delete("1001".getBytes());

            // 删除单个版本(回到上个版本)
            // delete.addColumn("info".getBytes(), "name".getBytes());

            // 删除所有版本
            delete.addColumns("info".getBytes(), "name".getBytes());

            // 删除列族
            // delete.addFamily("info".getBytes());

            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
