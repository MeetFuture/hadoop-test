package com.tangqiang.hbase.mapreduce;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase 数据统计
 *
 * @author tqiang
 * @email tqiang@grgbanking.com
 */
public class TQHBaseRowCount extends Configured implements Tool {
	private static Logger logger = LoggerFactory.getLogger(TQHBaseRowCount.class);
	private String jobName = "RowCount";
	private String quorumUrl = "master";
	private String clientPort = "2181";

	public int run(String[] args) throws Exception {
		logger.info("----------------------Begin HBase RowCount ---------------------");
		String tableName = args[1];
		logger.info("ToolRunner Begin Run ,  JobName:" + jobName + "  TableName:" + tableName);

		Configuration conf = HBaseConfiguration.create(getConf());
		conf.set("hbase.zookeeper.quorum", quorumUrl);
		conf.set("hbase.zookeeper.property.clientPort", clientPort);
		conf.set("tableName", tableName);
		conf.setBoolean("mapreduce.map.speculative", false);

		Job job = new Job(conf, jobName + "-" + tableName);
		job.setJarByClass(TQHBaseRowCount.class);
		Scan scan = new Scan();
		scan.setCaching(1000);
		scan.setCacheBlocks(false);
		scan.setFilter(new FirstKeyOnlyFilter());
		job.setOutputFormatClass(NullOutputFormat.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
		job.setNumReduceTasks(0);
		return ((job.waitForCompletion(true)) ? 0 : 1);
	}

	/**
	 * Mapper
	 */
	public static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
		private long count = 0; 
		private String hostname = "Host";


		@Override
		protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Result>.Context context) throws IOException, InterruptedException {
			this.hostname = getHostNameForLiunx();
			super.setup(context);
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
			count ++;
			if (count % 10000 == 0) {
				context.setStatus(hostname + " " + count);
			}
		}
		
		@Override
		protected void cleanup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Result>.Context context) throws IOException, InterruptedException {
			context.setStatus(hostname + " " + count);
			super.cleanup(context);
		}

		/**
		 * 获取主机名
		 * 
		 * @return
		 */
		private String getHostNameForLiunx() {
			try {
				try {
					return (InetAddress.getLocalHost()).getHostName();
				} catch (UnknownHostException uhe) {
					String host = uhe.getMessage();
					if (host != null) {
						int colon = host.indexOf(':');
						if (colon > 0) {
							return host.substring(0, colon);
						}
					}
					return "UnknownHost";
				}
			} catch (Exception e) {
				return "Error";
			}
		}
	}

}