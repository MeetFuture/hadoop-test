package com.tangqiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 单词统计
 *
 * @author tqiang
 * @email tqiang@grgbanking.com
 */
public class NumberCount {
	public static String name = "NumberCount";
	public static String desc = "A map/reduce used to counts the Number in the input files ! ";

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: NumberCount <in> [<in>...] <out>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, name);
		job.setJarByClass(NumberCount.class);
		job.setMapperClass(NumberCountMapper.class);
		job.setCombinerClass(NumberCountReducer.class);
		job.setReducerClass(NumberCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Mapper
	 */
	public static class NumberCountMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text number = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			int length = line.length();
			for (int i = 0; i < length; i++) {
				number.set("" + line.charAt(i));
				context.write(number, one);
			}
		}
	}

	public static class NumberCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}
