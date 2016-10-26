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
 * @ClassName：App2
 * @Description：求数字总和
 * @From http://blog.csdn.net/savechina/article/details/5835933
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年4月11日 下午7:06:39
 * 
 * 用Mapper读取文本文件用StringTokenizer对读取文件内的每一行的数字
 * （Hadoop处理文本文件时，处理时是一行一行记取的）
 * 进行分隔，获取每一个数字，然后求和,再将求得的值按Key/Value格式写入Context，
 * 最后用Reducer对求得中间值进行汇总求和，得出整个文件所有数字的和
 */
public class NumSum2 {
	
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
		job.setJarByClass(NumSum2.class);
		job.setMapperClass(SumMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("ok");
	}
	
	// 对每一行数据进行分隔，并求和
	public static class SumMapper extends Mapper<Object, Text, Text, LongWritable> {
		private Text word = new Text("sum");
		private static LongWritable numValue = new LongWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			long sum = 0;
			while (itr.hasMoreTokens()) {
				sum += Long.valueOf(itr.nextToken());
			}
			numValue.set(sum);
			context.write(word, numValue);
		}
	}
	
	// 汇总求和，输出  
	public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private Text word = new Text("sum");
		private LongWritable result = new LongWritable(1);
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(word, result);
		}
	}
	
}