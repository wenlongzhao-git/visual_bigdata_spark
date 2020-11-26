package com.bigdata.redis;

import com.bigdata.util.EntityUtil;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;
import java.util.*;

/**
 * Redis的并发访问类
 * @author mrq
 *
 */
public class RedisParallel3 implements Runnable{

	private static Logger log = Logger.getLogger(RedisParallel3.class);
	private static Properties props = new Properties();
	private List<String> resultList = null;
	private String redisip = null;
	private int redisport = 0;
	private String keyPattern;
	private int begindex = EntityUtil.REDIS_USER_LOCATION_KEY.length();

	static {
		try {
			props.load(RedisParallel3.class.getClassLoader().getResourceAsStream("redis.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public RedisParallel3(String redisip,int redisport,List<String> datalist,String keyPattern){
		this.resultList = datalist;
		this.redisip = redisip;
		this.redisport = redisport;
		this.keyPattern = keyPattern;
	}

	public static void main(String[] args) throws InterruptedException {
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		//并行获取keys
		Set<String> keys = findKeys(redisip,redisport,keyPattern);
		if(keys != null && !keys.isEmpty()){
			String[] arr = new String[keys.size()];
			keys.toArray(arr);//装载key到数组
			log.info(redisip+":"+redisport+"======readDataByPipeline");
			//并行获取数据
			List<String> rs = readDataByPipeline(arr);
			synchronized(resultList){
				resultList.addAll(rs);
			}
		}
		log.info(redisip+":"+redisport+"======used [" + (System.currentTimeMillis() - start)/1000  + "] seconds ..");
	}
	
	/**
	 * 从reids集群中批量获取数据
	 * @param keyPattern
	 * @param batchSize
	 * @return
	 */
	public static List<String> loadKVs(String keyPattern,String crn){
		log.info("keyPattern======"+keyPattern);
		long start = System.currentTimeMillis();
		String[] clusterIp = props.getProperty("redis.cluster"+crn+".ip").split(",");//集群配置
		List<String> result = new ArrayList<String>();//结果集
		List<Thread> ls = new ArrayList<Thread>();//线程队列
		for(String node : clusterIp){
			ls.add(new Thread(new RedisParallel3(node.split(":")[0], Integer.parseInt(node.split(":")[1]),result,keyPattern)));
		}
		if(ls != null && !ls.isEmpty()){
			log.info("threadSize======"+ls.size());
			for(Thread t : ls){
				t.start();
			}
			for(Thread t : ls){
				try {
					t.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		log.info("======result size:"+result.size());
		log.info("======redisJob used [" + (System.currentTimeMillis() - start)/1000  + "] seconds ..");
		return result;
	}

	/**
	 * 批量获取key值
	 * @param keyPattern
	 * @return
	 */
	private Set<String> findKeys(String redisip,int redisport,String keyPattern){
		long start = System.currentTimeMillis();
		Jedis redis = null;
		try{
			redis = new Jedis(redisip,redisport, 10000);//10秒超时
			Pipeline p = redis.pipelined();
			Response<Set<String>> responsesKey = p.keys(keyPattern);
			p.sync();
			Set<String> keys = responsesKey.get();//获得匹配的key值
			MyJedisPool.recycleJedisOjbect(redis);
			log.info(redisip+":"+redisport+"======keys size:[" + keys.size() + "] ..");
			log.info(redisip+":"+redisport+"======used [" + (System.currentTimeMillis() - start) + "] ms ..");
			return keys;
		}catch(Exception e){
			log.error("======"+e.getMessage(),e);
			log.error(redisip+":"+redisport+"======keys error");
			return null;
		}finally{
			if(redis != null){
				redis.close();
			}
		}
	}
	
	/**
	 * 通过pipeline读取数据
	 * @param datalist
	 * @return
	 */
	private List<String> readDataByPipeline(String[] datalist){
		long start = System.currentTimeMillis();
		Jedis redis = null;
		try{
			redis = new Jedis(redisip,redisport, 20000);
			Pipeline p = redis.pipelined();
			Map<String,Response<String>> responsesValue = new HashMap<String,Response<String>>();
			for(int i=0;i < datalist.length;i++){
				responsesValue.put(datalist[i], p.get(datalist[i]));
			}
			p.sync();
			List<String> result = new ArrayList<String>();
			String msg = null;
			for(String k : responsesValue.keySet()) {
				msg = responsesValue.get(k).get();
				if(msg != null && !msg.equals("")){
	//				result.add(k.substring(begindex) + EntityUtil.DB_RESULT_SPLITSTR + msg);
					result.add(k.substring(begindex) + EntityUtil.DB_RESULT_SPLITSTR + msg.split(EntityUtil.DB_RESULT_SPLITSTR)[0]);
				}
			}
			if(!result.isEmpty()){
				log.info(redisip+":"+redisport+"======"+result.get(0));
			}
			log.info(redisip+":"+redisport+"======result size:[" + result.size() + "] ..");
			log.info(redisip+":"+redisport+"======hgetAll with pipeline used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
			return result;
		}catch(Exception e){
			log.error("======"+e.getMessage(),e);
			log.error(redisip+":"+redisport+"======result error");
			return null;
		}finally{
			if(redis != null){
				redis.close();
			}
		}
	}

}
