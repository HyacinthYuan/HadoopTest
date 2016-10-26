package com.jet.hadoop.avg;

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
 * @ClassName：AvgTemperature
 * @Description：求每年平均气温，Mapper获取年份和气温数据，在Reducer中汇总求均值
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年4月16日 下午10:09:41
 */
public class AvgTemperature {

	public static void main(String[] args) throws Exception{
		
		String jobName = "avg_temperature_group_by_year";

		long timeBegin = System.currentTimeMillis();
		System.out.println("hadoop " + jobName + " begins at：" + timeBegin);
		
		Configuration conf = new Configuration();
		args = new String[3];
		args[0] = "E:\\Work\\input\\1901";
		args[1] = "E:\\Work\\input\\1902";
		args[2] = "E:\\Work\\output";
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: " + jobName + " <input path> [<input path>...] <output path>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(AvgTemperature.class);
		job.setMapperClass(AvgTemperatureMapper.class);
		job.setReducerClass(AvgTemperatureReducer.class);
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
	
	public static class AvgTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static final int MISSING = 9999;
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value == null) {
				return;
			}
			String line = value.toString();
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
	
	public static class AvgTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			int num = 0;
			for (IntWritable val : values) {
				sum += val.get();
				num++;
			}
			context.write(key, new IntWritable(new Long(sum / num).intValue()));
		}
	}
}
