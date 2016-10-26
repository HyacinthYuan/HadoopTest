package com.jet.hadoop.max;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @ClassName：MaxTempeYear
 * @Description：求得每年的最高气温 
 * 数据来源：https://github.com/tomwhite/hadoop-book/tree/master/input/ncdc/all
 * 官网数据下载地址：ftp://ftp.ncdc.noaa.gov/pub/data/noaa
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年4月16日 下午8:03:36
 */
public class MaxTemperature {

	public static void main(String[] args) throws Exception{
		
		String jobName = "max_temperature_group_by_year";

		long timeBegin = System.currentTimeMillis();
		System.out.println("hadoop " + jobName + " begins at：" + timeBegin);
		
		Configuration conf = new Configuration();
		if (args == null || args.length == 0) {
			args = new String[5];
			args[0] = "E:\\Work\\input\\ncdc\\1990.txt";
			args[1] = "E:\\Work\\input\\ncdc\\1991.txt";
			args[2] = "E:\\Work\\input\\ncdc\\1992.txt";
			args[3] = "E:\\Work\\input\\ncdc\\1993.txt";
			args[4] = "E:\\Work\\output";
		}
		
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: " + jobName + " <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(MaxTemperature.class);
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		
		boolean result = job.waitForCompletion(true);
		
		long timeEnd = System.currentTimeMillis();
		System.out.println("hadoop " + jobName + " ended at：" + timeEnd);
		System.out.println("hadoop " + jobName + " cost time：" + (timeEnd - timeBegin)/1000 + " seconds.");
		
		System.exit(result ? 0 : 1);
	}
	
	public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static final int MISSING = 9999;
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value == null) {
				return;
			}
			String line = value.toString();
			if (line.length() < 92) {
				return;
			}
			String year = line.substring(15, 19);
			int temperature;
			temperature = Integer.valueOf(line.substring(88, 92));
			if (temperature == MISSING) {
				return;
			}
			if ("-".equals(line.substring(87, 88))) {
				temperature *= -1;
			}
			context.write(new Text(year), new IntWritable(temperature));
		}
	}
	
	public static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int temp = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				temp = Math.max(temp, value.get());
			}
			context.write(key, new IntWritable(temp));
		}
	}

}
