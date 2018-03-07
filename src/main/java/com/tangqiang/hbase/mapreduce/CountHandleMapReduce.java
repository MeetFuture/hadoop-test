package com.tangqiang.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class CountHandleMapReduce extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		Job job = new Job(config, "ExampleRead");
		job.setJarByClass(CountHandleMapReduce.class); // class that contains mapper
		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob("REP_ALLCASH_REPORT", // input HBase table name SENT_SentInfosDetailByPubId
				scan, // Scan instance to control CF and attribute selection
				CountHandleMapper.class, // mapper
				Text.class, // mapper output key
				IntWritable.class, // mapper output value
				job);

		job.setNumReduceTasks(1); // at least one, adjust as required

		// 设置Map、Combine和Reduce处理类
		job.setCombinerClass(CountHandleReducer.class);
		job.setReducerClass(CountHandleReducer.class);

		FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/count"));

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		return ((b) ? 0 : 1);
	}

	public static class CountHandleMapper extends TableMapper<Text, IntWritable> {
		public void map(ImmutableBytesWritable key, Result value, Context context) throws InterruptedException, IOException {
			context.write(new Text("count"), new IntWritable(1));
		}
	}

	public static class CountHandleReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}