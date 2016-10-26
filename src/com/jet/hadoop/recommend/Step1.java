package com.jet.hadoop.recommend;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * @ClassName：Step1
 * @Description：按用户分组，计算所有物品出现的组合列表，得到用户对物品的评分矩阵 
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年6月21日 下午8:33:48
 */
public class Step1 {

	public static class Step1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		
		private final static IntWritable k = new IntWritable();
		private final static  Text v = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			String[] tokens = value.toString().split(",");
			k.set(Integer.parseInt(tokens[0]));
			v.set(tokens[1] + ":" + tokens[2]);
			output.collect(k, v);
		}
	}
	
	public static class Step1Reducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		
		private final static Text v = new Text();

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			StringBuilder sb = new StringBuilder();
			while (values.hasNext()) {
				sb.append(",");
				sb.append(values.next());
			}
			v.set(sb.toString().replaceFirst(",", ""));
			output.collect(key, v);
		}
	}
	
	public static void run(Map<String, String> path) throws IOException {
		JobConf conf = Recommender.getConf();
		
		String input = path.get("Step1Input");
		String output = path.get("Step1Output");
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Step1Mapper.class);
		conf.setCombinerClass(Step1Reducer.class);
		conf.setReducerClass(Step1Reducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		RunningJob job = JobClient.runJob(conf);
		while (!job.isComplete()) {
			job.waitForCompletion();
		}
	}
}
