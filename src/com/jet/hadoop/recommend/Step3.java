package com.jet.hadoop.recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @ClassName：Step3
 * @Description：合并同现矩阵和评分矩阵并相乘 
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年6月23日 上午9:51:50
 */
public class Step3 {
	
	public static class Step3Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		private static final IntWritable k = new IntWritable();
		private static final Text v = new Text();
		private String flag;
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();
		}
		
		@Override
		public void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			if (flag.equals("recommend")) {
				String[] values = value.toString().split(",");
				k.set(Integer.parseInt(values[1]));
				v.set("User:" + values[0] + "," + values[2]);
				context.write(k, v);
			}
			if (flag.equals("step2")) {
				String[] values = value.toString().split("\t");
				String[] items = values[0].split(":");
				k.set(Integer.parseInt(items[0]));
				v.set("Item:" + items[1] + "," + values[1]);
				context.write(k, v);
 			}	
		}
	}
	
	public static class Step3Reducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {
		
		private static final Text k = new Text();
		private static final DoubleWritable v = new DoubleWritable();
		
		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			Map<String, String> mapUser = new HashMap<String, String>();
			Map<String, String> mapItem = new HashMap<String, String>();
			
			for (Text text : values) {
				String val = text.toString();
				String[] vals = val.substring(5).split(",");
				if (val.startsWith("User:")) {	// 评分矩阵
					mapUser.put(vals[0], vals[1]);
				}
				if (val.startsWith("Item:")) {	// 同现矩阵
					mapItem.put(vals[0], vals[1]);
				}
			}
			
			// 这地方是关键之处，思路参考：http://f.dataguru.cn/thread-229459-1-1.html
			Iterator<String> itrItem = mapItem.keySet().iterator();
			while (itrItem.hasNext()) {	// 同现矩阵，与评分矩阵的先后顺序不能反了（否则会缺少很多项）
				String itemId = itrItem.next();
				int coTimes = Integer.parseInt(mapItem.get(itemId));
				Iterator<String> itrUser = mapUser.keySet().iterator();	// 需要注意！itrUser要多次遍历，每次遍历之前需重置
				while (itrUser.hasNext()) {	// 评分矩阵
					String userId = itrUser.next();
					double score = Double.parseDouble(mapUser.get(userId));
					k.set(userId + "," + itemId);
					v.set(score * coTimes);
					context.write(k, v);
				}
			}
		}
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		
		JobConf conf = Recommender.getConf();
		
		String input1 = path.get("Step3Input1");
		String input2 = path.get("Step3Input2");
		String output = path.get("Step3Output");
		
		Job job = Job.getInstance(conf);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapperClass(Step3Mapper.class);
//		job.setCombinerClass(Step3Reducer.class); // Reducer中的计算需要相同key的所有数据，不能直接使用Reducer作为Combiner
		job.setReducerClass(Step3Reducer.class);
		
		FileInputFormat.addInputPath(job, new Path(input1));
		FileInputFormat.addInputPath(job, new Path(input2));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
	}

}
