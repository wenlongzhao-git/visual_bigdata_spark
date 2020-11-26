package com.bigdata.task;

import com.bigdata.jdbc.DBOperation;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;


/**
 * 配置信息管理类
 * @author mrq
 *
 */
public class ConfigMgr {
	
	private static Logger log = Logger.getLogger(ConfigMgr.class);
	
	public static void main(String[] args) {
//		if (args.length < 3) {
//			System.err.println("Usage: ConfigMgr <filename>");
//			System.exit(1);
//		}
		long start = System.currentTimeMillis();
		outputConf1(args[0]);
		log.info("======used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
	}

	/**
	 * 输出位置数据到文件
	 * @param crn
	 * @param filename
	 */
	public static void outputConf1(String filename){
		log.info("======从Oracle读取基站扇区与grid的关系");
		List<String> locData = DBOperation.getFanGrid();
		log.info("======输出文件");
		long start = System.currentTimeMillis();
		createCSV(locData,filename);
		log.info("======文件生成    used [" + (System.currentTimeMillis() - start)/1000+"] seconds");
	}
	
	/**
	 * 输出csv文件
	 * @param ls
	 * @param filename
	 */
	public static void createCSV(List<String> ls,String filename) {
		BufferedWriter w = null;
		try {
			w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), "UTF-8"));
			for(String msg : ls){
				w.append(msg+"\n");
			}
		}  catch (Exception e) {
			log.error(e.getMessage(),e);
		} finally {
			try {
				w.flush();
				w.close();
			} catch (IOException e) {
				//e.printStackTrace();
			}

		}
	}
}
