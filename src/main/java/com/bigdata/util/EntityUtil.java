package com.bigdata.util;

import java.util.HashMap;
import java.util.Map;

/**
 * 常量及通用方法
 * @author mrq
 *
 */
public class EntityUtil {
	
	public static final String TOPIC_USER_LOCATION = "tpc_realtime_location1";//用户实时位置信令
	public static final String TOPIC_USER_LOCATION_SPLITSTR = "\\|";//用户实时位置信令分隔符
	public static final String REDIS_USER_LOCATION = "kv0001:proc:user-station:";//redis中用户实时位置
	public static final String REDIS_USER_LOCATION_KEY = "kv0001:proc:user-station:";//redis中用户实时位置
	public static final String ORACLE_PROC_DETAIL = "tb_proc_detail";//oracle中的过程明细
	public static final String TOPIC_USER_LOCATION_CHANGE = "topic_busi0001_base_user_location_change";//用户位置变化信息
	public static final String DATE_FORMAT_STR = "yyyyMMddHHmmss";//日期格式
	public static final String UNKOWNSTR = "NULL_VALUE";//空值的默认值
	public static final String DB_RESULT_SPLITSTR = ",";//计算结果写入数据库，内容中的分隔符
	public static final String SITE_FAN_SPLIT = "$";
	
	
	private static Map<String,String[]> metadata = new HashMap<String,String[]>();//元数据集
	static{
		//定义数据结构
		metadata.put(TOPIC_USER_LOCATION, "datasrc,msisdn,lac,ci,begintime,endtime,fstintime,intime,eventid,appid,websiteid".split(","));
		metadata.put(REDIS_USER_LOCATION, "msisdn,grid,lac,ci,endtime,uattr".split(","));
		//1|18616994978|43020|14581|20170425190252|20170425190452|20170425180614|20170425192411|123||
		
		metadata.put(ORACLE_PROC_DETAIL, "msisdn,site_fan,uattr".split(","));
	}
	
	/**
	 * 通过元数据构造实体类
	 * @param entityType 数据集
	 * @param split 数据的分隔符
	 * @param data 数据
	 * @return
	 */
	public static Map<String,String> createEntity(String entityType,String split,String data){
		if (data == null || data.equals("")){
			return null;
		}
		String[] meta = metadata.get(entityType);
		String[] dataArr = data.split(split);
		Map<String,String> returnValue = new HashMap<String,String>();
		for(int i=0;i<meta.length;i++){
			returnValue.put(meta[i], (i < dataArr.length ? (dataArr[i] == null ? "": dataArr[i]):""));
		}
		return returnValue;
	}

	/**
	 * 解析实体类，输出消息格式的字符串
	 * @param entityType 数据集
	 * @param split 数据的分隔符
	 * @param data 解析实体类
	 * @return
	 */
	public static String parseEntity(String entityType,String split,Map<String,String> data){
		if (data == null || data.isEmpty()){
			return null;
		}
		String[] meta = metadata.get(entityType);
		String msg = "";
		for(int i=0;i<meta.length;i++){
			msg += data.get(meta[i]) == null ? "" : data.get(meta[i]);
			if(i<meta.length-1){
				msg += split;
			}
		}
//		System.out.println(msg);
		return msg;
	}

	
	public static void main(String[] args) {
	}

}
