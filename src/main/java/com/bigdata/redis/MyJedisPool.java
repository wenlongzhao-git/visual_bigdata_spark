package com.bigdata.redis;

import redis.clients.jedis.*;
import redis.clients.util.JedisClusterCRC16;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutorService;

public class MyJedisPool implements Serializable{

	private static JedisPool pool;
	private static Set<HostAndPort> nodes = new HashSet<HostAndPort>();
	private static Map<String,HostAndPort> hostMap= new HashMap<String,HostAndPort>();
	private static JedisPoolConfig config = new JedisPoolConfig();
	private static Properties props = new Properties();
	private static ExecutorService fixedThreadPool = null;
	static {
		try {
			props.load(MyJedisPool.class.getClassLoader().getResourceAsStream("redis.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static MyJedisPool mypool = new MyJedisPool();
	
	private MyJedisPool(){
//		// 创建jedis池配置实例
//		config = new JedisPoolConfig();
//		// 设置池配置项值
//		config.setMaxTotal(Integer.valueOf(props.getProperty("jedis.pool.maxActive")));
//		config.setMaxIdle(Integer.valueOf(props.getProperty("jedis.pool.maxIdle")));
//		config.setMaxWaitMillis(Long.valueOf(props.getProperty("jedis.pool.maxWait")));
//		// 根据配置实例化jedis池
//		pool = new JedisPool(config, props.getProperty("redis.ip"),
//				Integer.valueOf(props.getProperty("redis.port")));
//		//实例化线程池
//		fixedThreadPool = Executors.newCachedThreadPool();
//		String[] clusterIp = props.getProperty("redis.cluster2.ip").split(",");
//		for(String node : clusterIp){
//			 nodes.add(new HostAndPort(node.split(":")[0], Integer.parseInt(node.split(":")[1]))); 
//		}
//		HostAndPort host = null;
//		for(int i = 0; i < clusterIp.length; i++){
//			host = new HostAndPort(clusterIp[i].split(":")[0], Integer.parseInt(clusterIp[i].split(":")[1]));
//			nodes.add(host);
//			hostMap.put(i+"", host);
//		}
		
	}
	
	public static MyJedisPool getInstance() {
		return mypool;
	}
	
	
//	public Map<String, HostAndPort> getHostMap() {
//		return hostMap;
//	}

	/** 获得jedis对象 */
	public Jedis getJedisObject() {
		Jedis jd = pool.getResource();
		return jd;
	}

	public JedisCluster getJedisCluster() {
		return new JedisCluster(nodes, config);
	}

	
	public ExecutorService getExecutorService(){
		return fixedThreadPool;
	}
	
	/** 归还jedis对象 */

	public static void recycleJedisOjbect(Jedis jedis) {
		if(jedis != null){
			jedis.close();
		}
		
	}

	/**
	 * 根据key值获得集群中host节点信息
	 * @param key
	 * @return
	 */
	public static HostAndPort getHostByKey(String key){
		return hostMap.get(getHostIndex(key));
	}
	
	/**
	 * 根据下标值获得集群中host节点信息
	 * @param idx
	 * @return
	 */
	public static HostAndPort getHostByIndex(String idx){
		return hostMap.get(idx);
	}

	/**
	 * 根据key值计算集群中host节点下标
	 * @param key
	 * @return
	 */
	public static String getHostIndex(String key){
		if(key == null){
			return null;
		}
		int slot = JedisClusterCRC16.getSlot(key);
		int idx = slot / 1024;
		int mod = slot % 1024;
		if(mod == 0 && idx > 0){
			idx--;
		}
		return idx+"";
	}
	
	/**
	 * 
	 * 测试jedis池方法
	 * 
	 */

	public static void main(String[] args) {
	}

}