package org.geekbang.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BaseTest {

    public static void main(String[] args) throws IOException {
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("student"); // 怎么没有namespace
        String colFamily1 = "info";
        String colFamily2 = "score";
        int rowKey1 = "Tom";
        int rowKey2 = "Jerry";
        int rowKey3 = "Jack";
        int rowKey4 = "Rose";
        int rowKey5 = "Yao"; // 中文好像不行

        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(colFamily1);
            hTableDescriptor.addFamily(hColumnDescriptor1);
            HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(colFamily2);
            hTableDescriptor.addFamily(hColumnDescriptor2);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        // 插入数据
        Put put1 = new Put(Bytes.toBytes(rowKey1)); // row key
        put1.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes(202100000000001)); // col1
        put1.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes(1)); // col2
        put1.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes(75)); // col3
        put1.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes(82)); // col4
        conn.getTable(tableName).put(put1);
        System.out.println("Data insert success");

        Put put2 = new Put(Bytes.toBytes(rowKey2)); // row key
        put2.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes(202100000000002)); // col1
        put2.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes(1)); // col2
        put2.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes(85)); // col3
        put2.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes(67)); // col4
        conn.getTable(tableName).put(put2);
        System.out.println("Data insert success");

        Put put3 = new Put(Bytes.toBytes(rowKey3)); // row key
        put3.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes(202100000000003)); // col1
        put3.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes(2)); // col2
        put3.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes(80)); // col3
        put3.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes(80)); // col4
        conn.getTable(tableName).put(put3);
        System.out.println("Data insert success");

        Put put4 = new Put(Bytes.toBytes(rowKey4)); // row key
        put4.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes(202100000000004)); // col1
        put4.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes(2)); // col2
        put4.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes(60)); // col3
        put4.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes(61)); // col4
        conn.getTable(tableName).put(put4);
        System.out.println("Data insert success");

        Put put5 = new Put(Bytes.toBytes(rowKey5)); // row key
        put5.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes('G20220735040038')); // col1
        put5.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes(3)); // col2
        put5.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes(90)); // col3
        put5.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes(90)); // col4
        conn.getTable(tableName).put(put5);
        System.out.println("Data insert success");

        // 查看数据（可能有问题）
        Get get = new Get(Bytes.toBytes(rowKey5));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey5));      // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }
}
