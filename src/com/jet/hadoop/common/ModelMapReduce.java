package com.jet.hadoop.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * @ClassName：ModelMapReduce
 * @Description：MapReduce编写模板 
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年4月17日 下午5:46:25
 */
public class ModelMapReduce extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new ModelMapReduce(), args);
		
		System.exit(status);
	}
	
	/**
	 * 
	 * @ClassName：ModelMapper Class
	 * 
	 */
	public static class ModelMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO
		}

		@Override
		public void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
		
	}
	
	/**
	 * 
	 * @ClassName：ModelReducer Class
	 */
	public static class ModelReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		
		@Override
		public void setup(
				Reducer<LongWritable, Text, LongWritable, Text>.Context context)
						throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
						throws IOException, InterruptedException {
			// TODO
		}
		
		@Override
		public void cleanup(
				Reducer<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	/**
	 * Driver 
	 */
	@Override
	public int run(String[] args) throws Exception {
		
		// 1) create job
		Job job = parseInputAndOutput(this, args);
		
		// 2) set job
		// step 1: set job run class
		job.setJarByClass(ModelMapReduce.class);
		
		// step 2: set input format
		job.setInputFormatClass(TextInputFormat.class);
		
		// step 3: set mapper class
		job.setMapperClass(ModelMapper.class);
		
		// step 4: set map output key/value class
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		// step 5: set shuffle(sort,combiner,group)
		// set sort
		job.setSortComparatorClass(LongWritable.Comparator.class);
		// set combiner(optional, default is unset), Subclass of Reducer
		// job.setCombinerClass(null);
		// set group
		job.setGroupingComparatorClass(LongWritable.Comparator.class);
		
		// step 6: set reducer class
		job.setReducerClass(ModelReducer.class);
		
		// step 7: set job output key/value class
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		// step 8: output format
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// 3) submit job
		boolean isCompleted = job.waitForCompletion(true);
		
		return isCompleted ? 0 : 1;
	}

	private Job parseInputAndOutput(Tool tool, String[] args) throws IOException {
		// validate
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", tool.getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return null;
		}
		
		// create job
		Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
		
		// set input path
		Path inputPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputPath);
		
		// set output path
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return job;
	}

}
