package com.jet.hadoop.logana;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @ClassName：LogAnalysis
 * @Description：黑马论坛日志分析项目（基于HDFS、Hive）
 * 参考：http://www.cnblogs.com/edisonchou/p/4449082.html 
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年6月17日 下午8:18:20
 */
public class LogAnalysis {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		long timeBegin = System.currentTimeMillis();
		System.out.println("begins at：" + timeBegin);
		
		if (args == null || args.length == 0) {
			args = new String[2];
			args[0] = "E:\\Work\\input\\HeimaBBS\\access_2013_05_30.log";
			args[1] = "E:\\Work\\output";	
		}
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Log Analysis");
		job.setJarByClass(LogAnalysis.class);
		job.setMapperClass(LogAnalysisMapper.class);
		job.setReducerClass(LogAnalysisReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		boolean result = job.waitForCompletion(true);
		
		long timeEnd = System.currentTimeMillis();
		System.out.println("ended at：" + timeEnd);
		System.out.println("cost time：" + (timeEnd - timeBegin)/1000 + " seconds.");
		
		System.exit(result ? 0 : 1);
		
	}

	public static class LogAnalysisMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		private LogParser logParser = new LogParser();
		private Text valueOut = new Text();
		
		@Override
		public void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			final String[] parsed = logParser.parse(value.toString());

            // step1.过滤掉静态资源访问请求
            if (parsed[2].startsWith("GET /static/")
                    || parsed[2].startsWith("GET /uc_server")
                    || parsed[2].startsWith("GET /source")
                    || parsed[2].startsWith("GET /images")
                    || parsed[2].startsWith("GET /template")
                    || parsed[2].startsWith("GET /favicon.ico")
                    || parsed[2].startsWith("GET / HTTP/1.1")
                    || parsed[2].startsWith("GET /data")) {
                return;
            }
            
            // step2.过滤掉开头的指定字符串
            if (parsed[2].startsWith("GET /")) {
                parsed[2] = parsed[2].substring("GET /".length());
            } else if (parsed[2].startsWith("POST /")) {
                parsed[2] = parsed[2].substring("POST /".length());
            }
            
            // step3.过滤掉结尾的特定字符串
            if (parsed[2].endsWith(" HTTP/1.1")) {
                parsed[2] = parsed[2].substring(0, parsed[2].length()
                        - " HTTP/1.1".length());
            }
            
            // step4.只写入前三个记录类型项
            valueOut.set(parsed[0] + "\t " + parsed[1] + "\t " + parsed[2]);
            context.write(key, valueOut);
		}
	}
	
	public static class LogAnalysisReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
		
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, 
				Reducer<LongWritable, Text, Text, NullWritable>.Context context) 
						throws IOException, InterruptedException {
			for (Text val: values) {
				context.write(val, NullWritable.get());
			}
		}
	}
	
	static class LogParser {
        public static final SimpleDateFormat FORMAT = new SimpleDateFormat(
                "d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        public static final SimpleDateFormat dateformat1 = new SimpleDateFormat(
                "yyyyMMddHHmmss");/**
         * 解析英文时间字符串
         * 
         * @param string
         * @return
         * @throws ParseException
         */
        private Date parseDateFormat(String string) {
            Date parse = null;
            try {
                parse = FORMAT.parse(string);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return parse;
        }

        /**
         * 解析日志的行记录
         * 
         * @param line
         * @return 数组含有5个元素，分别是ip、时间、url、状态、流量
         */
        public String[] parse(String line) {
            String ip = parseIP(line);
            String time = parseTime(line);
            String url = parseURL(line);
            String status = parseStatus(line);
            String traffic = parseTraffic(line);

            return new String[] { ip, time, url, status, traffic };
        }

        private String parseTraffic(String line) {
            final String trim = line.substring(line.lastIndexOf("\"") + 1)
                    .trim();
            String traffic = trim.split(" ")[1];
            return traffic;
        }

        private String parseStatus(String line) {
            final String trim = line.substring(line.lastIndexOf("\"") + 1)
                    .trim();
            String status = trim.split(" ")[0];
            return status;
        }

        private String parseURL(String line) {
            final int first = line.indexOf("\"");
            final int last = line.lastIndexOf("\"");
            String url = line.substring(first + 1, last);
            return url;
        }

        private String parseTime(String line) {
            final int first = line.indexOf("[");
            final int last = line.indexOf("+0800]");
            String time = line.substring(first + 1, last).trim();
            Date date = parseDateFormat(time);
            return dateformat1.format(date);
        }

        private String parseIP(String line) {
            String ip = line.split("- -")[0].trim();
            return ip;
        }
    }
	
}
