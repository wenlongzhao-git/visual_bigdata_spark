package com.bigdata.spark;

import com.shunicom.util.EntityUtil;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 位置识别类
 * @author mrq
 *
 */
public class LocationProc7 implements Runnable{

	private static Logger log = Logger.getLogger(LocationProc7.class);
	private static SimpleDateFormat df = new SimpleDateFormat(EntityUtil.DATE_FORMAT_STR);
	private static int timeout_sec = 1 * 60 * 60;//2小时的秒数，停留2小时即视为超时
	private String idx = null;
	private Jedis jedis = null;
	private List<String> recordList = null;
	
	public LocationProc7(){
		
	}
	/**
	 * 构造函数
	 * @param jedis
	 * @param recordList
	 */
	public LocationProc7(String idx, Jedis jedis, List<String> recordList){
		this.idx = idx;
		this.jedis = jedis;
		this.recordList = recordList;
	}
	
	public void run() {
		long start = System.currentTimeMillis();
		doProc();
		int size = 0;
		if(recordList != null && !recordList.isEmpty()){
			size = recordList.size();
			log.info(Thread.currentThread().getName()+"======doProc used [" 
					+ (System.currentTimeMillis() - start) + "] ms; idx="+idx+"; size="+size);
		}
	}
	
	/**
	 * 数据处理
	 */
	public void doProc() {
		if(recordList == null || recordList.isEmpty()){
			return;
		}
//		log.info(Thread.currentThread().getName()+"======8=idx="+idx+";size="+recordList.size());
		Map<String,String> tmp = null;
		String key = null;
		try{
			Pipeline p = jedis.pipelined();
			for(int i = 0;i < recordList.size();i++){
				tmp = EntityUtil.createEntity(EntityUtil.TOPIC_USER_LOCATION,EntityUtil.TOPIC_USER_LOCATION_SPLITSTR, recordList.get(i));
				if(tmp != null){
					key = EntityUtil.REDIS_USER_LOCATION_KEY + tmp.get("msisdn");
					//数据结构：lac$ci,信令结束时间,入库时间
					p.set(key,tmp.get("lac") + EntityUtil.SITE_FAN_SPLIT + tmp.get("ci")
							+ EntityUtil.DB_RESULT_SPLITSTR + df.format(new Date()) + EntityUtil.DB_RESULT_SPLITSTR + recordList.get(i));
					//key的过期时间
					p.expire(key, timeout_sec);
				}
			}
			p.sync();
		}catch(Exception e){
			log.error("======"+e.getMessage(),e);
			log.error(Thread.currentThread().getName()+"======doProc error; idx="+idx);
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
		
	}
	
}
