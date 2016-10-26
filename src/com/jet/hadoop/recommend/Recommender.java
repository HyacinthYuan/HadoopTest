package com.jet.hadoop.recommend;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;

import com.jet.util.FileUtil;

/**
 * @ClassName：Recommender
 * @Description：基于物品的电影推荐系统 
 * 
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年6月21日 下午8:21:23
 */
public class Recommender {
	
	public static String DIR_PREFIX;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if (args == null || args.length == 0) {
			DIR_PREFIX = "E:/Work/app";
		} else {
			DIR_PREFIX = "hdfs://mybdtest01:9000";
		}
		String rootDir = DIR_PREFIX + "/recommend";

		Map<String, String> path = new HashMap<String, String>();
		path.put("data", "small.txt");
		path.put("Step1Input", rootDir);
		path.put("Step1Output", rootDir + "/step1");
		path.put("Step2Input", path.get("Step1Output"));
		path.put("Step2Output", rootDir + "/step2");
		path.put("Step3Input1", path.get("Step1Input") + "/" + path.get("data"));
		path.put("Step3Input2", path.get("Step2Output"));
		path.put("Step3Output", rootDir + "/step3");
		path.put("Step4Input", path.get("Step3Output"));
		path.put("Step4Output", rootDir + "/step4");
		
		if (args == null || args.length == 0) {
			if (new File(path.get("Step1Output")).exists()) {
				FileUtil.removeAll(path.get("Step1Output"));
			}
			if (new File(path.get("Step2Output")).exists()) {
				FileUtil.removeAll(path.get("Step2Output"));
			}
			if (new File(path.get("Step3Output")).exists()) {
				FileUtil.removeAll(path.get("Step3Output"));
			}
			if (new File(path.get("Step4Output")).exists()) {
				FileUtil.removeAll(path.get("Step4Output"));
			}
		}
		
		Step1.run(path);
		Step2.run(path);
		Step3.run(path);
		Step4.run(path);
	}
	
	public static JobConf getConf() {
		JobConf conf = new JobConf(Recommender.class);
		conf.setJobName("Recommender");
		return conf;
	}

}
