package com.bigdata.task;

import com.bigdata.util.EntityUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * 在spark中使用hbase原生API访问
 * 
 * @author mrq
 *
 */
public class SparkTest {

	private static Logger log = Logger.getLogger(SparkTest.class);
	public static final String TB_HISTORY = "tb_shurta_result_history";
	public static final String TB_HISTORY_CF = "current";
	public static final String TB_HISTORY_CF_Q1 = "labval";
//	public static final String POS_STAT_PATH = "/root/test/pos_stat/";//本机环境
	public static final String POS_STAT_PATH = "/data/shurta/pos_stat/test/";//测试服务器环境
//	public static final String POS_STAT_PATH = "hdfs://hacluster/shurta/test/pos_stat/";
	public static final String ROWKEY_SPLITSTR = "-";
	public static final String LABEL_VALUE_SPLITSTR = "##";
	public static final String LABEL_VALUE_REDUCE_SPLITSTR = "@;";

	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("sparkHBase");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum", "192.168.137.34:2181");
//		jsc.hadoopConfiguration().set("hbase.zookeeper.quorum", "192.168.137.34:2181");
//		jsc.hadoopConfiguration().set(TableOutputFormat.OUTPUT_TABLE,TB_HISTORY);
//		try {
//			Job job = Job.getInstance(jsc.hadoopConfiguration());
//			job.setOutputKeyClass(ImmutableBytesWritable.class);
//		    job.setOutputValueClass(Result.class);
//		    job.setOutputFormatClass(TableOutputFormat.class);		
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		// 广播日期变量
//		String dateStr = "20171102143000";
//		final Broadcast<String> dateBroadcast = jsc.broadcast(dateStr);
//		final Broadcast<Configuration> confBroadcast = jsc.broadcast(conf);
		
		try {
			/*
				lac$ci,label_name,label_value,pcnt
				3181$19645,ufrom,520000,1
				$4173711,uv,uv,77
				43044$11182,ufrom,340000,1
				3185$21211,uv,uv,93
				3189$49452,flux_used,1-100MB,6
				$478692,equip_brand,other,1
				3187$15243,age,18岁-25岁,2
				3189$35201,arpu,200元以上,1
				43028$20964,flux_used,1024MB以上,4
				43024$13111,equip_net,other,2
				5194$24719,age,18岁-25岁,5
			 */
			JavaRDD<String> statFan = jsc.textFile(POS_STAT_PATH);// 从hdfs读取基站扇区级别的统计数据
			log.info("将statFan转化成JavaPairRDD");//key=lac$ci-标签名;   value=标签值$$人数
			JavaPairRDD<String, String> statFanPair = prepareRDDForDB(statFan);
			
			/*
			 * 根据key合并值
			 * 
			 * key=43044$11182-gender,value=男$$10
			 * key=43044$11182-gender,value=女$$5
			 * 转化成
			 * key=43044$11182-gender,value=女$$;男$$10
			 * 
			 * @param result
			 * @return
			 */
			log.info("将statFanPair进行Reduce，合并相同相同key中标签值，形成json字符串，为hbase存储做准备");//key=lac$ci,标签名,日期;   value=标签值,人数
			JavaPairRDD<String, String> rsFan = combineResult(statFanPair);
			
			/*
			 * 将结果写入hbase
			 */
//			writeToDbByNewAPI(rsFan,jsc,dateBroadcast.getValue());
			writeToDB(rsFan);
		}
		finally {
			  jsc.stop();
		}
	}

	/**
	 * 根据hbase结构组合key
	 * @param statrdd
	 * @return
	 */
	public static JavaPairRDD<String, String> prepareRDDForDB(JavaRDD<String> statRdd){
		JavaPairRDD<String, String> rs = statRdd.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] arr = arg0.split(EntityUtil.DB_RESULT_SPLITSTR);
				return new Tuple2<String, String>(arr[0] + ROWKEY_SPLITSTR + arr[1], 
						arr[2] + LABEL_VALUE_SPLITSTR + arr[3]);
			}
		});
		return rs;
	}
	
	/**
	 * 根据key合并值
	 * 
	 * key=43044$11182-gender,value=男##10
	 * key=43044$11182-gender,value=女##5
	 * 转化成
	 * key=43044$11182-gender,value=女##5@;男##10
	 * 
	 * @param result
	 * @return
	 */
	public static JavaPairRDD<String, String> combineResult(JavaPairRDD<String, String> result){
		JavaPairRDD<String, String> rs = result.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String v1, String v2) throws Exception {
				return v1 + LABEL_VALUE_REDUCE_SPLITSTR + v2;
			}});
		return rs;
	}

	/**
	 * 结果写入hbase数据库
	 * @param data
	 */
	public static void writeToDB(JavaPairRDD<String, String> data){
		data.foreachPartition(new VoidFunction<Iterator<Tuple2<String,String>>>() {
			@Override
			public void call(Iterator<Tuple2<String,String>> v1) throws Exception {
				log.info("=============建立hbase连接 开始"+new Date());
				Configuration conf = HBaseConfiguration.create();
//				conf.set("hbase.zookeeper.quorum", "192.168.137.34");
				conf.set("hbase.zookeeper.quorum", "192.10.45.101");
				conf.set("hbase.zookeeper.property.clientPort", "6379");
				Connection connection = ConnectionFactory.createConnection(conf);
				log.info("=============建立hbase连接 完成"+new Date());
				Table table = connection.getTable(TableName.valueOf(TB_HISTORY));
				Tuple2<String,String> obj = null;
				List<Put> puts = new ArrayList<Put>();
				try {
					 while(v1.hasNext()){
						obj = v1.next();
						String rowid = obj._1() + ROWKEY_SPLITSTR + "20170102000000";//行键
			//			String[] values = obj._2().split(LABEL_VALUE_REDUCE_SPLITSTR,2);
						Put put = new Put(Bytes.toBytes(rowid));
						put.addColumn(Bytes.toBytes(TB_HISTORY_CF), Bytes.toBytes(TB_HISTORY_CF_Q1), Bytes.toBytes(obj._2()));
						puts.add(put);
					 }
					 table.put(puts);
				 } finally {
				   table.close();
				   connection.close();
				 }
			}
			
		});
	}

	/**
	 * 结果写入hbase数据库
	 * @param data
	 */
	public static void writeToDbByNewAPI(JavaPairRDD<String, String> data,JavaSparkContext jsc,
			final String dateBroadcastStr){
		data.mapToPair(new PairFunction<Tuple2<String,String>,ImmutableBytesWritable,Put>(){
			@Override
			public Tuple2<ImmutableBytesWritable,Put> call(Tuple2<String, String> v1) throws Exception {
				String rowkey = v1._1() + ROWKEY_SPLITSTR + dateBroadcastStr;//行键
				String value = v1._2();
	//			String[] values = v1._2().split(LABEL_VALUE_REDUCE_SPLITSTR,2);
				log.info("===============rowkey="+rowkey+";value="+value);
				Put put = new Put(Bytes.toBytes(rowkey));
				put.addColumn(Bytes.toBytes(TB_HISTORY_CF), Bytes.toBytes(TB_HISTORY_CF_Q1), Bytes.toBytes(v1._2()));
				return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
			}
		});
		//.saveAsNewAPIHadoopDataset(jsc.hadoopConfiguration());
	}

}
