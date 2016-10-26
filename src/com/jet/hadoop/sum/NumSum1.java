package com.jet.hadoop.sum;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @ClassName：Sum
 * @Description：计算文件中所有数字相加的总和
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * 自己的实现
 * @date 2016年4月10日 下午12:19:44
 */
public class NumSum1 {

	public static void main(String[] args) throws Exception{
		
		String jobName = "sum_number";

		long timeBegin = System.currentTimeMillis();
		System.out.println("hadoop " + jobName + " begins at：" + timeBegin);
		
		Configuration conf = new Configuration();
		args = new String[2];
		args[0] = "E:\\Work\\input\\numbers.txt";
		args[1] = "E:\\Work\\output";
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(NumSum1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		
		boolean result = job.waitForCompletion(true);
		
		long timeEnd = System.currentTimeMillis();
		System.out.println("hadoop " + jobName + " ended at：" + timeEnd);
		System.out.println("hadoop " + jobName + " cost time：" + (timeEnd - timeBegin)/1000 + " seconds.");
		
		System.exit(result ? 0 : 1);
	}
	
	// 拆分数字，所有数字的key相同（这样能保证reduce的时候汇总成一个key-value对）
	public static class TokenizerMapper extends
		Mapper<Object, Text, IntWritable, LongWritable> {
	
		private final static IntWritable one = new IntWritable(1);
		private LongWritable num = new LongWritable();
	
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				num.set(Long.valueOf(itr.nextToken()));
				context.write(one, num);
			}
		}
	}
	
	// 汇总求和，输出
	public static class LongSumReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
		
		LongWritable result = new LongWritable();
		
		@Override
		protected void reduce(IntWritable key, Iterable<LongWritable> value, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : value) {
				sum += key.get() * val.get();
			}
			result.set(sum);
			context.write(new IntWritable(1), result);
		}
	}


}
