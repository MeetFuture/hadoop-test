package com.tangqiang.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapreduce简单实例
 *
 * @author tqiang
 * @email tqiang@grgbanking.com
 */
public class MapReduceTest {
	protected Logger logger = LoggerFactory.getLogger(getClass());
	private static String inputPath = "hdfs://master:8020/test/tom/20150130/12/36";
	private static String outputPath = "hdfs://master:8020/test/result/";

	public static void main(String[] args) throws Exception {
		System.out.println("----------------------2----------------------");
		Configuration conf = new Configuration();
		// 这句话很关键
		//conf.set("mapred.job.tracker", "master:9001");
		conf.set("fs.default.name", "hdfs://master:8020");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.address", "master:8050");
		String[] otherArgs = new GenericOptionsParser(conf, null).getRemainingArgs();
		
		Job job = new Job(conf, "Number Count");
		job.setJarByClass(MapReduceTest.class);
		// 设置Map、Combine和Reduce处理类
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		// 设置输出类型

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(LongWritable.class);

		// 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
		job.setInputFormatClass(TextInputFormat.class);
		// 提供一个RecordWriter的实现，负责数据输出
		job.setOutputFormatClass(TextOutputFormat.class);
		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			String line = value.toString();
			int length = line.length();
			for (int i = 0; i < length; i++) {
				context.write(new Text("" + line.charAt(i)), new IntWritable(1));
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long sum = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				sum += iterator.next().get();// 计算总数
			}
			context.write(key, new LongWritable(sum));
		}

	}
}
