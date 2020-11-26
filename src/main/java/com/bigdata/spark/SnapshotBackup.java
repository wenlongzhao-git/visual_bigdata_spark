package com.bigdata.spark;

import com.shunicom.util.AppProfile;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

//全局配置

public class SnapshotBackup extends SnapshotProc2 implements Serializable{

	private static Logger log = Logger.getLogger(SnapshotBackup.class);
	public static final Properties PROPS = AppProfile.getInstance().getProps();//全局配置
	private String appName = "SnapshotProc2";//job默认名称
	
	/**
	 * 入口函数
	 * @param args
	 */
	public static void main(String[] args) {
		Map<String,String> param = getParaMap(args);
		if(param == null){
			log.info("======Please input param");
			return;
		}
		
		SnapshotProc2 snp = new SnapshotBackup();
		String appname = param.get("appName");
		if(appname != null && !appname.equals("")){
			snp.setAppName(appname);
		}
		
		long start = System.currentTimeMillis();
		log.info("======procName:"+param.get("procName"));
		if("default".equals(param.get("procName"))){
			log.info("======doDefault ...");
			snp.doDefault(param);
		}
		log.info("======used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
	}

	/**
	 * 设置参数
	 * @param args
	 * @return
	 */
	private static Map<String,String> getParaMap(String[] args){
		if(args.length > 0){
			Map<String,String> map = new HashMap<String,String>();
			if("default".equals(args[0]) ){
				if(args.length != 4){
					System.err.println("Usage: SnapshotBackup <procName> <crn> <currTime> <appName>");
					System.exit(1);
				}
				map.put("procName", args[0]);
				map.put("crn", args[1]);
				map.put("currTime", args[2]);
				map.put("appName", args[3]);
			}
			return map;
		}
		return null;
	}
	
	/**
	 * default方法重写
	 */
	public void doDefault(Map<String,String> param){
		if(param == null){
			log.error("参数param为空");
			return;
		}
		doDefault(param,null);
	}

	/**
	 * default方法重写，仅保留快照沉淀的逻辑
	 */
	protected void doDefault(Map<String,String> param, Map<String,List<String>> disList){
		if(param == null){
			log.error("参数param为空");
			return;
		}
		
		/*
		 * 获取当前快照数据
		 * 18601723326,lac$ci
		 * 18601723327,lac$ci
		 * 18601723328,lac$ci
		 */
		List<String> locDataList = getLocData(param);//从redis获取位置快照
		
		JavaSparkContext jsc = doInit();//初始化
		try{
			/*
			 * 创建快照RDD
			 * 快照数据加密、转换,持久化到hdfs
			 * 73CD34AC44E37A62982440ECD8C3A67B,lac$ci,1860172,20170101120000
			 * E441B62395FE07A0A83E6B6725EA62D3,lac$ci,1860172,20170101120000
			 * 17BF5832F7A0CFBC40C8772821E1A97C,lac$ci,1860172,20170101120000
			 */
			createSnapshot(jsc, locDataList, param, PROPS.getProperty("POS_SNAPSHOT_BAK_PATH"));//持久化快照
			long start = System.currentTimeMillis();
			log.info("======[defualt] used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
		}
		catch(Exception e){
			log.error("SnapshotBackup error",e);
		}
		finally{
			doRelease(jsc);//释放资源
		}
	}
}
