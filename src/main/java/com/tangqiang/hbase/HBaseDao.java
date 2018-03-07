package com.tangqiang.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase数据库操作
 *
 * @author tqiang
 * @email tqiang@grgbanking.com
 */
public class HBaseDao {
    private static Configuration conf = HBaseConfiguration.create();
    private static Connection connection = null;
    private static Admin admin = null;

    static {
        conf.set("hbase.rootdir", "hdfs://tom/hbase");
        // 设置Zookeeper,直接设置IP地址
        conf.set("hbase.zookeeper.quorum", "192.168.1.131,192.168.80.132,192.168.1.133");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 创建表
    public static void createTable(String tablename, String columnFamily) {
        TableName tableNameObj = TableName.valueOf(tablename);
        try {
            if (admin.tableExists(tableNameObj)) {
                System.out.println("Table exists!");
                System.exit(0);
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(tableDesc);
                System.out.println("create table success!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 删除表
    public static void deleteTable(String tableName) {
        try {
            TableName table = TableName.valueOf(tableName);
            admin.disableTable(table);
            admin.deleteTable(table);
            System.out.println("delete table " + tableName + " ok.");
        } catch (IOException e) {
            System.out.println("删除表出现异常！");
        }
    }

    // 插入一行记录
    public static void put(String tableName, String rowKey, String family, String qualifier, String value) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            table.close();
            //System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //查询数据
    public static String get(String tableName, String rowKey, String family, String qualifier) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            //通过rowKey实例化G
            Get get = new Get(Bytes.toBytes(rowKey));
            //添加列族名和列名条件
            get.addColumn(family.getBytes(), qualifier.getBytes());
            //执行Get，返回结果
            Result result = table.get(get);
            //返回结果
            return Bytes.toString(result.getValue(family.getBytes(), qualifier.getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    //统计记录数
    public static long count(String tableName) {
        try {
            final long[] rowCount = {0};
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner resultScanner = table.getScanner(scan);
            resultScanner.forEach(result -> {
                rowCount[0] += result.size();
                //result.size()是int型
            });
            return rowCount[0];
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * 扫描表
     */
    public static List<String> scan(String tableName, String startRow, String stopRow, String family, String qualifier) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            //初始化Scan
            Scan scan = new Scan();
            //指定开始的rowKey
            scan.setStartRow(startRow.getBytes());
            //指定结束的rowKey
            scan.setStopRow(stopRow.getBytes());
            scan.addColumn(family.getBytes(), qualifier.getBytes());
            //执行scan返回结果
            ResultScanner result = table.getScanner(scan);
            List<String> list = new ArrayList<>();
            String value = null;
            for (Result r : result) {
                value = Bytes.toString(r.getValue(family.getBytes(), qualifier.getBytes()));
                list.add(value);
            }
            return list;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 扫描表
     */
    public static List<String> scan(String tableName, String family, String qualifier) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            //初始化Scan
            Scan scan = new Scan();
            scan.addColumn(family.getBytes(), qualifier.getBytes());
            //执行scan返回结果
            ResultScanner result = table.getScanner(scan);
            List<String> list = new ArrayList<>();
            String value = null;
            for (Result r : result) {
                value = Bytes.toString(r.getValue(family.getBytes(), qualifier.getBytes()));
                list.add(value);
            }
            return list;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static List<String> scan(String tableName) {
        List<String> list = new ArrayList<>();
        try {
            //获取表
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            StringBuffer sb = null;
            for (Result result : resultScanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    sb = new StringBuffer();
                    sb.append("rowKey:").append(Bytes.toString(CellUtil.cloneRow(cell))).append("\t");
                    sb.append("family:").append(Bytes.toString(CellUtil.cloneFamily(cell))).append(",");
                    sb.append(Bytes.toString(CellUtil.cloneQualifier(cell))).append("=");
                    sb.append(Bytes.toString(CellUtil.cloneValue(cell)));
                    list.add(sb.toString());
                }
            }
            return list;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void delete(String tableName, String rowKey, String family, String qualifier) {
        try {
            //获取表
            Table table = connection.getTable(TableName.valueOf(tableName));
            //通过rowKey实例化Delete
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            //指定列族名、列名和值
            delete.addColumn(family.getBytes(), qualifier.getBytes());
            //执行Delete
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭
     */
    public static void close() {
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //测试
    public static void main(String[] args) {
        HBaseDao.deleteTable("testA");
        HBaseDao.createTable("testA", "info");
        //循环插入10条数据
        for (int i = 0; i < 10; i++) {
            HBaseDao.put("testA", "00" + i, "info", "name", "test" + i);
            HBaseDao.put("testA", "00" + i, "info", "age", i + "");
        }
        System.out.println("count=" + HBaseDao.count("testA"));
        String value = HBaseDao.get("testA", "001", "info", "name");
        System.out.println("value=" + value);
        //扫描
        System.out.println("------------------sacn(testA,000,004,info,name)");
        List<String> list = HBaseDao.scan("testA", "000", "004", "info", "name");
        for (String s : list) {
            System.out.println(s);
        }
        //扫描
        System.out.println("------------------sacn(testA,info,name)");
        list = HBaseDao.scan("testA", "info", "name");
        for (String s : list) {
            System.out.println(s);
        }
        list.clear();
        //扫描
        System.out.println("------------------sacn(testA)");
        list = HBaseDao.scan("testA");
        for (String s : list) {
            System.out.println(s);
        }
        HBaseDao.close();
    }
}
