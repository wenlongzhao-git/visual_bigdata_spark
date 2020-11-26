
package com.bigdata.spark;

import com.bigdata.redis.MyJedisPool2;
import com.bigdata.util.EntityUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ExecutorService;


/**
 * 实时位置信令留接入 从kafka的tpc_realtime_location中读取位置信令,间隔期2秒
 * 
 * @author mrq
 *
 */

public final class LocationStreaming7 {
	private static Logger log = Logger.getLogger(LocationStreaming7.class);
//	private static long duration_time = 1000;// 窗口间隙默认1秒
//	private static int batch_size_per_thread = 15000;// 多线程写入redis,每个线程处理的消息数
//	private static SimpleDateFormat df = new SimpleDateFormat(EntityUtil.DATE_FORMAT_STR);
	private static volatile Broadcast<MyJedisPool2> redisPool = null;

	private LocationStreaming7() {
	}

	public static void main(String[] args) {
		if (args.length < 6) {
			System.err.println("Usage: LocationStreaming <brokerList> <group> <topics> <numThreads> <duration> <redis-cluster-id> <kafka-sasl>");
			System.exit(1);
		}
		System.setProperty("java.security.auth.login.config", args[6]);
		System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

		SparkConf sparkConf = new SparkConf().setAppName("LocationStreaming7-"+args[1])
				.set("spark.executor.extraJavaOptions","-Djava.security.auth.login.config="+args[6]);

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(Long.parseLong(args[4])));
		redisPool = jssc.sparkContext().broadcast(MyJedisPool2.getInstance(args[5]));//将连接池广播
				
		int numThreads = Integer.parseInt(args[3]);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		Set<String> topicSet = new HashSet<String>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicSet.add(topic);
			topicMap.put(topic, numThreads);
		}
		Collection<String> topicss = Arrays.asList(topics);
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", args[0]);
		kafkaParams.put("group.id", args[1]);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("enable.auto.commit", false);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");
		kafkaParams.put("security.protocol","SASL_PLAINTEXT");
		kafkaParams.put("sasl.mechanism","GSSAPI");
		kafkaParams.put("sasl.kerberos.service.name","kafka");

		 JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(
						jssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe(topicss, kafkaParams)
				);

		JavaPairDStream<String, String> streams = stream.mapToPair(
				new PairFunction<ConsumerRecord<String, String>, String, String>() {
					@Override
					public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
						return new Tuple2<>(record.key(), record.value());
					}
				});

		JavaDStream<String> line = streams.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> v2) throws Exception {
				return v2._2();
			}
		});
		/*line.count().foreachRDD(new VoidFunction<JavaRDD<Long>>() {
			@Override
			public void call(JavaRDD<Long> longJavaRDD) throws Exception {
				longJavaRDD.saveAsTextFile("hdfs://beh001/user/beho/shurta/bbb/");
			}
		});

		DStream<String> dstream = line.dstream();*/

		log.info("从kafka中获取消息，间隔时间为" + args[4] + "ms");
		JavaPairDStream<String,String> lines1 = line.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				return new Tuple2<String, String>(arg0.split(EntityUtil.TOPIC_USER_LOCATION_SPLITSTR)[1], arg0);
			}
		});
		

		/**
		 * 根据信令时间取最新数据
		 */

		JavaPairDStream<String,String> lines2 = lines1.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String s1, String s2) {return s2;}
		});

		lines2.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
			@Override
			public void call(JavaPairRDD<String, String> data) throws Exception {
				List<Tuple2<String,String>> datalist = data.collect();
				String index = null;//分片数据集索引
				Map<String,List<String>> dataMap = new HashMap<String,List<String>>();//将数据按照key值得slot值进行分片
				List<String> recordList = null;//分片数据
				for(Tuple2<String,String> record : datalist){
					index = MyJedisPool2.getHostIndex(EntityUtil.REDIS_USER_LOCATION_KEY+record._1());
					if(index != null){
						if(dataMap.containsKey(index)){
							recordList = dataMap.get(index);
						}else{
							recordList = new ArrayList<String>();
							dataMap.put(index, recordList);
						}
						recordList.add(record._2());
					}
				}
				log.info("datalist.size()======"+datalist.size()+";dataMapSize======"+dataMap.size());
				MyJedisPool2 jdpool = redisPool.getValue();
				ExecutorService fixedThreadPool = jdpool.getExecutorService();
				for(String idx : dataMap.keySet()){
					HostAndPort host = MyJedisPool2.getHostByIndex(idx);
					Jedis redis = new Jedis(host.getHost(),host.getPort(),5000);//5秒超时
					fixedThreadPool.execute(new LocationProc7(idx,redis,dataMap.get(idx)));
				}
			}
		});

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

