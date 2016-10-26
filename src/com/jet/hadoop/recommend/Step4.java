package com.jet.hadoop.recommend;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @ClassName：Step4
 * @Description：TODO 
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年6月23日 下午1:10:20
 */
public class Step4 {

	public static class Step4Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		private static final Text k = new Text();
		private static final DoubleWritable v = new DoubleWritable();
		
		@Override
		public void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] vals = value.toString().split("\t");
			k.set(vals[0]);
			v.set(Double.parseDouble(vals[1]));
			context.write(k, v);
		}
	}
	
	public static class Step4Reducer extends Reducer<Text, DoubleWritable, IntWritable, Text> {
		
		private static final IntWritable k = new IntWritable();
		private static final Text v = new Text();
		
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] keys = key.toString().split(",");
			double result = 0;
			for (DoubleWritable value : values) {
				result += value.get();
			}
			k.set(Integer.parseInt(keys[0]));
			v.set(keys[1] + "," + result);
			context.write(k, v);
		}
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		JobConf conf = Recommender.getConf();
		
		String input = path.get("Step4Input");
		String output = path.get("Step4Output");
		
		Job job = Job.getInstance(conf);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Step4Mapper.class);
		job.setReducerClass(Step4Reducer.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
	}
	
}
