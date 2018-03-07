package com.tangqiang.hbase.insert;

import com.tangqiang.TestConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * hbase 入库测试
 *
 * @author tqiang
 * @email tqiang@grgbanking.com
 */
public class HBaseInsert {
    public static String name = "HBaseInsert";
    public static String desc = "Used to test the speed about hbase insert !";

    private Logger logger = LoggerFactory.getLogger(getClass());
    private AtomicLong count = new AtomicLong();
    private int lastCount = 0;
    // private String hbaseMaster = "master:60010";

    private TableName tableName = TestConfig.tableName;

    public static void main(String[] args) throws Exception {
        System.err.println("		Usage: HBaseInsert test <threads> <data count> ...");
        System.err.println("		Usage: HBaseInsert clean ...");
        if (args.length < 1) {
            //System.exit(2);
            args = new String[]{"test", "10", "1000000"};
        }
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", TestConfig.quorom);
        config.set("hbase.zookeeper.property.clientPort", TestConfig.clientPort);
        // config.set("hbase.master", hbaseMaster);
        // config.set("zookeeper.znode.parent", "/hbase");

        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();// 将参数中的设置信息添加到 conf 中

        if ("clean".equals(args[0])) {
            new HBaseInsert().reBuildTable(config);
        } else if ("test".equals(args[0])) {
            new HBaseInsert().excuteInsert(otherArgs, config);
        }
    }

    public void excuteInsert(String[] args, Configuration config) throws Exception {
        logger.info("................begin excute insert.....................................................");
        logger.info("Begin connect to table [" + tableName + "] ... ");

        Connection hConnection = ConnectionFactory.createConnection(config);
        Admin admin = hConnection.getAdmin();
        Table hTable = hConnection.getTable(tableName);
        logger.info("Connect to table [" + tableName + "] success ! Result : " + hTable);

        TableName[] tableNames = admin.listTableNames();
        for (TableName tableN : tableNames) {
            logger.info("	ListTableName : " + tableN);
        }

        hConnection.close();

        int threads = Integer.valueOf(args[1]);
        int dataCounts = Integer.valueOf(args[2]);
        int datas = dataCounts / threads;
        logger.info("Begin insert data to table [" + tableName + "]  Threads:" + threads + "  DataCount:" + dataCounts + "  ... ");
        long beginTime = System.currentTimeMillis();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            pool.execute(new HBaseInsertThread(config, datas));
        }
        pool.shutdown();
        while (!pool.awaitTermination(50, TimeUnit.MILLISECONDS)) {
            int nowPercent = (int) (count.get() / (dataCounts / 10));
            if (nowPercent != lastCount) {
                lastCount = nowPercent;
                logger.info(" Already insert " + count + " data to table : " + tableName + "  Speed: " + count.get() / ((double) (System.currentTimeMillis() - beginTime) / 1000) + " persecs");
            }
        }
        long usedTime = System.currentTimeMillis() - beginTime;
        logger.info("Data insert success ! TimeUsed:" + usedTime + "/ms  Speed:" + dataCounts / ((double) usedTime / 1000) + " persecs");

    }

    /**
     * 重建表
     *
     * @param config
     * @throws Exception
     */
    private void reBuildTable(Configuration config) throws Exception {
        logger.info("................begin excute reBuildTable.....................................................");
        Connection connection = ConnectionFactory.createConnection(config);
        Admin hAdmin = connection.getAdmin();
        if (hAdmin.tableExists(tableName)) {
            if (!hAdmin.isTableDisabled(tableName)) {
                logger.info("Begin disable table [" + tableName + "] ....");
                hAdmin.disableTable(tableName);
            }
            logger.info("Begin delete table [" + tableName + "] ....");
            hAdmin.deleteTable(tableName);
        }
        logger.info("Building table [" + tableName + "] ....");
        TableDescriptorBuilder tableDesc = TableDescriptorBuilder.newBuilder(tableName);
        tableDesc.setMemStoreFlushSize(8 * 1024 * 1024);
        tableDesc.setValue(TableDescriptorBuilder.SPLIT_POLICY, KeyPrefixRegionSplitPolicy.class.getName());
        tableDesc.setValue("prefix_split_key_policy.prefix_length", "2");
        tableDesc.addCoprocessor("org.apache.hadoop.hbase.observer.AggregateImplementation");

        ColumnFamilyDescriptorBuilder nameColumnDesc = ColumnFamilyDescriptorBuilder.newBuilder("name".getBytes());
        nameColumnDesc.setInMemory(true);
        nameColumnDesc.setTimeToLive(360);
        nameColumnDesc.setKeepDeletedCells(KeepDeletedCells.FALSE);
        nameColumnDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX);
        nameColumnDesc.setCompressionType(Algorithm.SNAPPY);
        nameColumnDesc.setEvictBlocksOnClose(true);
        tableDesc.addColumnFamily(nameColumnDesc.build());

        ColumnFamilyDescriptorBuilder infoColumnDesc = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes());
        infoColumnDesc.setInMemory(false);
        infoColumnDesc.setTimeToLive(360);
        infoColumnDesc.setKeepDeletedCells(KeepDeletedCells.FALSE);
        infoColumnDesc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        infoColumnDesc.setCompressionType(Algorithm.SNAPPY);
        infoColumnDesc.setEvictBlocksOnClose(true);
        tableDesc.addColumnFamily(infoColumnDesc.build());

        hAdmin.createTable(tableDesc.build(), Bytes.toBytes("A"), Bytes.toBytes("Z"), 27);
        hAdmin.close();
        logger.info("................Re BuildTable success.....................................................");
    }

    /**
     * 执行插入数据线程
     */
    private class HBaseInsertThread extends Thread {
        private int dataCount;
        private Connection hConnection;

        public HBaseInsertThread(Configuration config, int dataCount) throws Exception {
            this.dataCount = dataCount;
            this.hConnection = ConnectionFactory.createConnection(config);
        }

        @Override
        public void run() {
            try {
                Table table = hConnection.getTable(tableName);
                for (int i = 1; i <= dataCount; i++) {
                    byte[] byteRow = Bytes.toBytes(getNameData());
                    Put put = new Put(byteRow);
                    put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("name"), byteRow);
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(getAgeData()));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("height"), Bytes.toBytes(getHeightData()));
                    //执行Put
                    table.put(put);
                    count.getAndIncrement();
                }
                //关闭表和连接
                table.close();
                hConnection.close();
            } catch (Exception e) {
                logger.error("Data insert Error !", e);
            }

        }

        private String getNameData() {
            String[] ABC = new String[]{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"};
            return ABC[(int) (Math.random() * ABC.length)] + ABC[(int) (Math.random() * ABC.length)] + (int) (Math.random() * 1000) + System.currentTimeMillis();
        }

        private int getAgeData() {
            return (int) (Math.random() * 99 + 1);
        }

        private int getHeightData() {
            return (int) (Math.random() * 30 + 150);
        }

    }
}
