package com.jet.hadoop.sum;


import java.io.IOException;  
import java.util.StringTokenizer;  

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
 * @ClassName：App3
 * @Description：求数字总和
 * @From http://blog.csdn.net/savechina/article/details/5835933 
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年4月11日 下午7:07:17
 * 
 * 用Mapper读取文本文件用StringTokenizer对文件内的数字进行分隔，获取每一个数字，
 * 并救出文件中该数字有多少个，在合并过程中，求出每个数字在文件中的和，
 * 最后用Reducer对求得每个数字求得的和进行汇总求和，得出整个文件所有数字的和
 */
public class NumSum3 {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		args = new String[2];
		args[0] = "E:\\Work\\input\\numbers.txt";
		args[1] = "E:\\Work\\output";
		String[] otherArgs = new GenericOptionsParser(conf, args)  
                .getRemainingArgs();
		if (otherArgs.length != 2) {  
            System.err.println("Usage: numbersum <in> <out>");  
            System.exit(2);  
        }
		Job job = new Job(conf, "number sum");
		job.setJarByClass(NumSum3.class);
		job.setMapperClass(SumMapper.class);
		job.setCombinerClass(SumCombiner.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("ok");
	}
	
	// 对每一个数字进行分隔  
	public static class SumMapper extends Mapper<Object, Text, Text, LongWritable> {
		private Text word = new Text();
		private static LongWritable numValue = new LongWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, numValue);
			}
		}
	}
	
	// 对每一个数字进行汇总求和
	// combiner会被递归调用
	public static class SumCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
		private Text k = new Text("midsum");
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			if (!key.toString().startsWith("midsum")) {
				for (LongWritable val : values) {
					sum += val.get();
				}
				long kval = Long.parseLong(key.toString());
				result.set(kval * sum);
				context.write(k, result);
			} else {
				for (LongWritable val : values) {
					context.write(key, val);
				}
			}
		}
	}
	
	// 汇总求和，输出  
	public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private Text k = new Text("sum");
		private LongWritable result = new LongWritable();
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(k, result);
		}
	}
	
}