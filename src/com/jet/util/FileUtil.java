package com.jet.util;

import java.io.File;

/**
 * @ClassName：FileUtil
 * @Description：TODO 
 * @author YuanJie Email: JSYJIE@FOXMAIL.COM
 * @date 2016年6月23日 下午4:31:55
 */
public class FileUtil {
	public static void removeAll(String path) {
		File file = new File(path);
		if (file.isDirectory()) {
			String[] children = file.list();
			for (String folder : children) {
				String newPath = path + "/" + folder;
				removeAll(newPath);
			}
			file.delete();
 		} else {
 			file.delete();
 		}
	}
	
	public static void main(String[] args) {
		removeAll("E:/Work/app/recommend/step1");
	}
}
