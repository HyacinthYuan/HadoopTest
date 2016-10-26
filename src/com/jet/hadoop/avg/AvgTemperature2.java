package com.jet.hadoop.avg;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @ClassName：AvgTemperature2
 * @Description：求年平均气温，Mapper获取年份和气温数据，利用Combiner先求局部的和及值的数目，最后在Reducer中汇总求均值
 * @From：http://www.cnblogs.com/shishanyuan/archive/2014/12/22/4177908.html
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年4月17日 上午8:39:36
 */
public class AvgTemperature2 {
	public static void main(String[] args) throws Exception {

		args = new String[3];
		args[0] = "E:\\Work\\input\\1901";
		args[1] = "E:\\Work\\input\\1902";
		args[2] = "E:\\Work\\output";
        
        if(args.length != 3) {
            System.out.println("Usage: AvgTemperatrue <input path><output path>");
            System.exit(-1);
        }
       
        Job job = new Job();
        job.setJarByClass(AvgTemperature2.class);
        job.setJobName("Avg Temperature");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
       
        job.setMapperClass(AvgTemperatureMapper.class);
        job.setCombinerClass(AvgTemperatureCombiner.class);
        job.setReducerClass(AvgTemperatureReducer.class);
       
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
       
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
       
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
	
	public static class AvgTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
		 
	    private static final int MISSING = 9999;
	   
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	       
	        String line = value.toString();
	        String year = line.substring(15, 19);
	       
	        int airTemperature;
	        if(line.charAt(87) == '+') {
	            airTemperature = Integer.parseInt(line.substring(88, 92));
	        } else {
	            airTemperature =  Integer.parseInt(line.substring(87, 92));
	        }
	       
	        String quality = line.substring(92, 93);
	        if(airTemperature != MISSING && quality.matches("[01459]")) {
	            context.write(new Text(year), new Text(String.valueOf(airTemperature)));
	        }
	    }
	}
	
	public static class AvgTemperatureCombiner extends Reducer<Text, Text, Text, Text>{
		 
	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	       
	        double sumValue = 0;
	        long numValue = 0;
	       
	        for(Text value : values) {
	            sumValue += Double.parseDouble(value.toString());
	            numValue ++;
	        }
	       
	        context.write(key, new Text(String.valueOf(sumValue) + ',' + String.valueOf(numValue)));
	    }
	}
	
	public static class AvgTemperatureReducer extends Reducer<Text, Text, Text, IntWritable>{
		 
	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	       
	        double sumValue = 0;
	        long numValue = 0;
	        int avgValue = 0;
	       
	        for(Text value : values) {
	            String[] valueAll = value.toString().split(",");
	            sumValue += Double.parseDouble(valueAll[0]);
	            numValue += Integer.parseInt(valueAll[1]);
	        }
	       
	        avgValue  = (int)(sumValue/numValue);
	        context.write(key, new IntWritable(avgValue));
	    }
	}
}
