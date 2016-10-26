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
 * @ClassName：Step2
 * @Description：对物品组合列表进行计数，建立物品的同现矩阵 
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年6月22日 上午10:31:01
 */
public class Step2 {
	
	public static class Step2Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static Text k = new Text();
		private final static IntWritable v = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String[] tokens = value.toString().split("\t")[1].split(",");
			for (int i = 0; i < tokens.length; i++) {
				for (int j = 0; j < tokens.length; j++) {
					k.set(tokens[i].split(":")[0] + ":" + tokens[j].split(":")[0]);
					output.collect(k, v);
				}
			}
		}
	}
	
	public static class Step2Reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int result = 0;
			while (values.hasNext()) {
				result += values.next().get();
			}
			output.collect(key, new IntWritable(result));
		}
	}
	
	public static void run(Map<String, String> path) throws IOException {
		JobConf conf = Recommender.getConf();
		
		String input = path.get("Step2Input");
		String output = path.get("Step2Output");
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Step2Mapper.class);
		conf.setCombinerClass(Step2Reducer.class);
		conf.setReducerClass(Step2Reducer.class);
		
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
