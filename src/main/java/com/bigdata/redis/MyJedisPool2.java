package com.bigdata.redis;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.util.JedisClusterCRC16;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MyJedisPool2 implements Serializable{

	private static MyJedisPool2 mypool = null;
	private static Set<HostAndPort> nodes = new HashSet<HostAndPort>();
	private static Map<String,HostAndPort> hostMap= new HashMap<String,HostAndPort>();
	private static JedisPoolConfig config = new JedisPoolConfig();
	private static Properties props = new Properties();
	private static ExecutorService fixedThreadPool = null;
	
	static {
		try {
			props.load(MyJedisPool2.class.getClassLoader().getResourceAsStream("redis.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private MyJedisPool2(String clusterid){
		// 创建jedis池配置实例
		config = new JedisPoolConfig();
		// 设置池配置项值
		config.setMaxTotal(Integer.valueOf(props.getProperty("jedis.pool.maxActive")));
		config.setMaxIdle(Integer.valueOf(props.getProperty("jedis.pool.maxIdle")));
		config.setMaxWaitMillis(Long.valueOf(props.getProperty("jedis.pool.maxWait")));
		//实例化线程池
		fixedThreadPool = Executors.newCachedThreadPool();
		String[] clusterIp = props.getProperty("redis.cluster"+clusterid+".ip").split(",");
		HostAndPort host = null;
		for(int i = 0; i < clusterIp.length; i++){
			host = new HostAndPort(clusterIp[i].split(":")[0], Integer.parseInt(clusterIp[i].split(":")[1]));
			nodes.add(host);
			hostMap.put(i+"", host);
		}
		
	}
	
	public static synchronized MyJedisPool2 getInstance(String clusterid) {
		if(mypool == null){
			mypool = new MyJedisPool2(clusterid);
		}
		return mypool;
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
//	    JedisCluster cluster = MyJedisPool2.getInstance().getJedisCluster();  
//	    String name1 = cluster.get("test1");
//	    System.out.println(name1);
//	    String name2 = cluster.get("test2");
//	    System.out.println(name2);
		String key = "kv0001:proc:user-station:"+"18601721234";
//		String key = "";
//		int slot = JedisClusterCRC16.getSlot(key);
//		System.out.println(slot);
//		redis.cluster2.slot=0-1024,1025-2048,2049-3072,3073-4096,4097-5120,5121-6144,6145-7168,7169-8192,8193-9216,9217-10240,10241-11264,11265-12288,12289-13312,13313-14336,14337-15360,15361-16383
//		int slot = 0;
//		int cnt = slot/1024;
//		int mod = slot % 1024;
//		System.out.println(cnt);
//		System.out.println(mod);
//		HostAndPort host = getHostByKey(key);
//		System.out.println(host.getHost() + ":" + host.getPort());
		
		String s = "18616994978|17岁以下|3|4";
		String[] arr = s.split("\\|",3+2);
		for(String str : arr){
			System.out.println(str);
		}
		
	}

}