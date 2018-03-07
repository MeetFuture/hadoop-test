package com.tangqiang.hbase.count;

import com.tangqiang.TestConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统计HBase表数据量
 *
 * @author tqiang
 * @email tqiang@grgbanking.com
 */
public class HBaseCountByRegion {
    public static String name = "HBaseCount-Region";
    public static String desc = "Used to count hbase data !";

    private Logger logger = LoggerFactory.getLogger(getClass());
    private AtomicLong count = new AtomicLong();
    private int lastCount = 0;

    private TableName tableName = TestConfig.tableName;
    private int interval = 10000;

    public static void main(String[] args) throws Exception {
        System.out.println("		Usage: HBaseCount-Region <table> <interval default 10000>...");
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", TestConfig.quorom);
        config.set("hbase.zookeeper.property.clientPort", TestConfig.clientPort);
        // 将参数中的设置信息添加到 conf 中
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

        new HBaseCountByRegion().excuteCount(otherArgs, config);
    }


    public void excuteCount(String[] args, Configuration config) throws Exception {
        logger.info("................begin excute HBaseCount.....................................................");
        tableName = args.length > 0 ? TableName.valueOf(args[0]) : tableName;
        interval = args.length > 1 ? Integer.valueOf(args[1]) : interval;
        logger.info("Begin connect to table [" + tableName + "] ... ");

        Connection hConnection = ConnectionFactory.createConnection(config);
        Table hTable = hConnection.getTable(tableName);
        logger.info("Connect to table [" + tableName + "] success ! Result : " + hTable);

        List<RegionInfo> listLocations = hConnection.getAdmin().getRegions(tableName);

        logger.info("	Table [" + tableName + "] has [" + listLocations.size() + "] RegionLocations ");
        hConnection.close();

        long beginTime = System.currentTimeMillis();
        ExecutorService pool = Executors.newFixedThreadPool(listLocations.size());
        for (RegionInfo regionInfo : listLocations) {
            pool.execute(new HBaseCountThread(config, regionInfo));
        }
        pool.shutdown();
        while (!pool.awaitTermination(50, TimeUnit.MILLISECONDS)) {
            int nowPercent = (int) (count.longValue() / interval);
            if (nowPercent != lastCount) {
                lastCount = nowPercent;
                logger.info(" Already count " + count + " data of table : " + tableName);
            }
        }
        long usedTime = System.currentTimeMillis() - beginTime;
        logger.info("Data count success ! Count:" + count.longValue() + "  TimeUsed:" + usedTime + "/ms  Speed:" + count.longValue() / ((double) usedTime / 1000) + " persecs");
    }

    private class HBaseCountThread extends Thread {
        private Configuration config;
        private RegionInfo hRegionInfo;

        public HBaseCountThread(Configuration config, RegionInfo hRegionInfo) {
            this.config = config;
            this.hRegionInfo = hRegionInfo;
        }

        @Override
        public void run() {
            try {
                //logger.info("Start Thread StartKey [" + Bytes.toString(hRegionInfo.getStartKey()) + "]  EndKey [" + Bytes.toString(hRegionInfo.getEndKey()) + "]");
                // logger.info("Test table opertion begin....................");
                // boolean b = hTable.exists(new Get(Bytes.toBytes("A123")));
                // logger.info("Test table opertion .................................. Result : " + b);
                Connection hConnection = ConnectionFactory.createConnection(config);
                Table hTable = hConnection.getTable(tableName);

                Scan scan = new Scan();
                scan.addFamily(Bytes.toBytes("name"));
                scan.setStartRow(hRegionInfo.getStartKey());
                scan.setStopRow(hRegionInfo.getEndKey());
                scan.setCaching(1000);
                scan.setCacheBlocks(false);

                ResultScanner scanner = hTable.getScanner(scan);
                for (Result result : scanner) {
                    // result.getRow()
                    // for (Cell kv : result.rawCells()) {// 遍历每一行的各列
                    // logger.info(" Data Qualifier:" + Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()) + "  Value:" + Bytes.toString(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()) + "  Row:"
                    // + Bytes.toString(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()));
                    // }
                    count.getAndIncrement();
                }
                hConnection.close();
            } catch (Exception e) {
                logger.error("Count Data Error !", e);
            }
        }
    }
}
