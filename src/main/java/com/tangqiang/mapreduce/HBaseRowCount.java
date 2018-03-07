package com.tangqiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.tangqiang.TestConfig;

/**
 * HBase 数据统计
 *
 * @author tqiang
 * @email tqiang@grgbanking.com
 */
public class HBaseRowCount {
	public static String name = "HBaseRowCount";
	public static String desc = "A map/reduce used to count the hbase table rows , result is in the job message ! ";

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: HBaseRowCount <table name>");
			System.exit(2);
		}
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		conf.set("hbase.zookeeper.quorum", TestConfig.quorom);
		conf.set("hbase.zookeeper.property.clientPort", TestConfig.clientPort);
		String tableName = otherArgs[2];
		conf.set("tableName", tableName);
		conf.setBoolean("mapreduce.map.speculative", false);

		Job job = new Job(conf, name + "-" + tableName);
		job.setJarByClass(HBaseRowCount.class);
		Scan scan = new Scan();
		scan.setCaching(10);
		scan.setCacheBlocks(false);
		scan.setFilter(new FirstKeyOnlyFilter());

		job.setOutputFormatClass(NullOutputFormat.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Mapper
	 */
	public static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {

		@Override
		public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {

		}
	}
}