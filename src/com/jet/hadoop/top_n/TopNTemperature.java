package com.jet.hadoop.top_n;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @ClassName：TopNTemperature
 * @Description：求输入气温数据中温度最高的前N个气温
 * 思路：找出最高的N个气温，定义一个N+1维的数组，将数据放在数据第一个位置后对数组进行排序，
 *       这样数组后5个元素为当前最高值；
 * Mapper负责处理splits，Reducer负责处理Mapper的输出，需设置Reducer的数量为1；
 * 思路来源：http://my.oschina.net/u/1378204/blog/343666
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年4月23日 上午11:41:48
 */
public class TopNTemperature extends Configured implements Tool {
	
	private static final int MISSING = 9999;

	public static void main(String[] args) throws Exception {

		if (args == null || args.length == 0) {			
			args = new String[2];
			args[0] = "E:\\Work\\input\\ncdc\\1990.txt";
			args[1] = "E:\\Work\\output";
		}
		
		String jobName = TopNTemperature.class.getSimpleName();
		long timeBegin = System.currentTimeMillis();
		System.out.println("hadoop " + jobName + " began at：" + timeBegin);
		
		int status = ToolRunner.run(new TopNTemperature(), args);

		long timeEnd = System.currentTimeMillis();
		System.out.println("hadoop " + jobName + " ended at：" + timeEnd);
		System.out.println("hadoop " + jobName + " cost time：" + (timeEnd - timeBegin)/1000 + " seconds.");
		System.exit(status);

	}
	
	public static class TopNMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
		int size;
		int top[];
		IntWritable out = new IntWritable();
		
		// 一个task开始时调用一次，对应一个split
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			size = context.getConfiguration().getInt("N", 10);
			top = new int[size + 1];
		}
		
		// 一个split中的一行调用一次
		@Override
		public void map(LongWritable key, Text value, Context context)
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
			
			top[0] = temperature;
			Arrays.sort(top);
		}
		
		// 一个task结束时调用一次，对应一个split
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			for (int i = 1; i <= size; i++) {
				out.set(top[i]);
				context.write(out, out);
			}
		}
	}
	
	public static class TopNReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		int size;
		int top[];
		IntWritable keyOut = new IntWritable();
		IntWritable valueOut = new IntWritable();
		
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			size = context.getConfiguration().getInt("N", 10);
			top = new int[size + 1];
		}
		
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			for (IntWritable val : values) {
				top[0] = val.get();
				Arrays.sort(top);
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			for (int i = size; i > 0 ;i--) {
				keyOut.set(size - i + 1);
				valueOut.set(top[i]);
				context.write(keyOut, valueOut);;
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		conf.setInt("N", 5);
		
		Job job = parseInputAndOutput(this, args);
		job.setJarByClass(TopNTemperature.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(TopNMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(TopNReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		boolean isCompleted = job.waitForCompletion(true);
		return isCompleted ? 0 : 1;
	}

	private Job parseInputAndOutput(Tool tool, String[] args) throws IOException {
		if (args.length < 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", tool.getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return null;
		}
		Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
		
		for (int i = 0; i < args.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		
		return job;
	}

}
