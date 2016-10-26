package com.jet.hadoop.secondary_sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName：SecondarySortNum
 * @Description：二次排序，输入数据每行两个整数，结果先按第一个数排序，再按第二个数排序；
 * 先利用分区对第一字段排序，再利用分区内的比较对第二字段排序
 * 思路来源：Hadoop源码自带的example 
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年4月24日 下午12:37:28
 */

/**
 * 在map阶段，使用job.setInputFormatClass定义的InputFormat将输入的数据集分割成小数据块splites，
 * 同时InputFormat提供一个RecordReder的实现。本例子中使用的是TextInputFormat，他提供的RecordReder会将文本的一行的行号作为key，
 * 这一行的文本作为value。这就是自定义Map的输入是<LongWritable, Text>的原因。然后调用自定义Map的map方法，
 * 将一个个<LongWritable, Text>对输入给Map的map方法。注意输出应该符合自定义Map中定义的输出<IntPair, IntWritable>。
 * 最终是生成一个List<IntPair, IntWritable>。在map阶段的最后，会先调用job.setPartitionerClass对这个List进行分区，
 * 每个分区映射到一个reducer。每个分区内又调用job.setSortComparatorClass设置的key比较函数类排序。
 * 可以看到，这本身就是一个二次排序。如果没有通过job.setSortComparatorClass设置key比较函数类，则使用key的实现的compareTo方法。
 * 在第一个例子中，使用了IntPair实现的compareTo方法，而在下一个例子中，专门定义了key比较函数类。
 * 在reduce阶段，reducer接收到所有映射到这个reducer的map输出后，也是会调用job.setSortComparatorClass设置的key比较函数类对所有数据对排序。然后开始构造一个key对应的value迭代器。
 * 这时就要用到分组，使用jobjob.setGroupingComparatorClass设置的分组函数类。只要这个比较器比较的两个key相同，他们就属于同一个组，
 * 它们的value放在一个value迭代器，而这个迭代器的key使用属于同一个组的所有key的第一个key。最后就是进入Reducer的reduce方法，
 * reduce方法的输入是所有的（key和它的value迭代器）。同样注意输入与输出的类型必须与自定义的Reducer中声明的一致。 
 */

/**
 * 通过实现分区（Partitioner）策略可以将具有某些特征的键分配到同一个reduce中；通过实现一个分组（Group）策略，
 * 可以保证具有某些特征的键的值会被组合到一起（组合在一起后，也就是reduce阶段中的那个可迭代对象）；
 * 然后最后实现了一个排序（Sort）策略，这也就是所谓的关键所在了，我们可以提供某种策略来控制键的排序。
 */
public class SecondarySortNum extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		
		if (args == null || args.length == 0) {			
			args = new String[2];
			args[0] = "E:\\Work\\input\\numPairs.txt";
			args[1] = "E:\\Work\\output";
		}
		
		String jobName = SecondarySortNum.class.getSimpleName();
		long timeBegin = System.currentTimeMillis();
		System.out.println("hadoop " + jobName + " began at：" + timeBegin);
		
		int status = ToolRunner.run(new SecondarySortNum(), args);

		long timeEnd = System.currentTimeMillis();
		System.out.println("hadoop " + jobName + " ended at：" + timeEnd);
		System.out.println("hadoop " + jobName + " cost time：" + (timeEnd - timeBegin)/1000 + " seconds.");
		System.exit(status);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = parseInputAndOutput(this, args);
		job.setJarByClass(SecondarySortNum.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MapperClass.class);
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(ReducerClass.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setOutputKeyClass(Text.class);
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
	
	public static class IntPair implements WritableComparable<IntPair> {
		
		private int first = 0;
		private int second = 0;
		
		public void set(int left, int right) {
			first = left;
			second = right;
		}
		
		public int getFirst() {
			return first;
		}
		
		public int getSecond() {
			return second;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
		    first = in.readInt();
		    second = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(first);
			out.writeInt(second);
		}
		
		@Override
		public int hashCode() {
			return first * 157 + second;
		}
		
		@Override
		public boolean equals(Object right) {
			if (right instanceof IntPair) {
				IntPair r = (IntPair) right;
				return r.first == first && r.second == second;
			} else {
				return false;
			}
		}
		
		public static class Comparator extends WritableComparator {
			public Comparator() {
				super(IntPair.class);
			}
			
			public int compare(byte[] b1, int s1, int l1,
                    byte[] b2, int s2, int l2) {
				return compareBytes(b1, s1, l1, b2, s2, l2);
			}
		}
		
		static {
			WritableComparator.define(IntPair.class, new Comparator());
		}

		@Override
		public int compareTo(IntPair o) {
			if (first != o.first) {
				return first < o.first ? -1 : 1;
			} else if (second != o.second) {
				return second < o.second ? -1 : 1;
			} else {
				return 0;
			}
		}
		
	}
	
	// 只根据第一个字段分区（确定交给哪个Reducer处理）
	public static class FirstPartitioner extends Partitioner<IntPair, IntWritable> {

		@Override
		public int getPartition(IntPair key, IntWritable value,
				int numPartitions) {
			return Math.abs(key.getFirst() * 127) % numPartitions;
		}
		
	}
	
	// 只根据第一个字段分组（确定一个key对应的value迭代器的values）
	public static class FirstGroupingComparator implements RawComparator<IntPair> {

		@Override
		public int compare(IntPair o1, IntPair o2) {
			int l = o1.getFirst();
			int r = o2.getFirst();
			return l == r ? 0 : (l < r ? -1 : 1);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	        return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, 
	                                             b2, s2, Integer.SIZE/8);
	    }
		
	}
	
	public static class MapperClass extends Mapper<LongWritable, Text, IntPair, IntWritable> {
		
		private final IntPair outKey = new IntPair();
		private final IntWritable outValue = new IntWritable();
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			int left = 0;
			int right = 0;
			if (itr.hasMoreTokens()) {
				left = Integer.parseInt(itr.nextToken());
				if (itr.hasMoreTokens()) {
					right = Integer.parseInt(itr.nextToken());
				}
				outKey.set(left, right);
				outValue.set(right);
				context.write(outKey, outValue);
			}
		}
	}
	
	public static class ReducerClass extends Reducer<IntPair, IntWritable, Text, IntWritable> {
		
		private static final Text SEPARATOR = new Text("------------------------------------------------");
		private final Text outKey = new Text();
		
		@Override
		public void reduce(IntPair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(SEPARATOR, null);
			outKey.set(Integer.toString(key.getFirst()));
			for (IntWritable val : values) {
				context.write(outKey, val);
			}
		}
	}

}
