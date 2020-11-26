package com.bigdata.spark;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shunicom.jdbc.DBOperation;
import com.shunicom.redis.RedisParallel3;
import com.shunicom.util.AppProfile;
import com.shunicom.util.DateUtil;
import com.shunicom.util.EntityUtil;
import com.shunicom.util.MD5EncryptTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * 周期性记录全网用户快照信息（默认周期15分钟）
 * 对快照进行标签计算
 * 用户标签样例：13262616747|30岁-39岁|男|0-49元|华为|4G|1024MB以上
 * 
 * @author mrq
 *
 */
public class SnapshotProc2 implements Serializable{

	private static Logger log = Logger.getLogger(SnapshotProc2.class);
	private static final String SUMSTR = "uv";//总人数标签名
	private static final String UFROM = "ufrom";//号段归属地标签名
	private static final String UFROM_SHORT = "ufrom_short";//号段归属地标签名，精简版仅包含上海，外省和国际三类
	private static final String UFROM_SHORT_VALUE_LOCAL = "310000";//精简版归属地，上海
	private static final String UFROM_SHORT_VALUE_NON_LOCAL = "999999";//精简版归属地，外省
	private static final String PRE_STR_CANTON = "CAN";//行政区划统计结果的key值前缀
	private static final String KEY_DIS_ALL = "DIS_ALL";//全市统计结果的key值
	private static final String SPLITSTR_UFROM = ",";
	private static final int SPLIT_LIMIT_UFROM = 2;//归属地字段个数
	
	//标签数据支持两种分隔符，根据app.properties中的LABLE_FILE.n.SPLITSTR_INDEX确定
	private static final String[] SPLITSTR_LABEL = {"\\|",","};

	//快照存放路径
	private static final String DIR_STAT_FAN = "fan";//基站扇区维度统计结果
	private static final String DIR_STAT_GRID = "grid";//栅格维度统计结果
	private static final String DIR_STAT_SUB = "sub";//订阅区域维度统计结果

//	private JavaSparkContext sc = null;//JavaSparkContext
	private String appName = "SnapshotProc2";//job默认名称
	public static final Properties PROPS = AppProfile.getInstance().getProps();//全局配置
	
	//hbase相关常量
	public static final String TB_HISTORY = "beho:tb_shurta_result_history";//历史快照计算结果存放表名
	public static final String TB_HISTORY_CF = "current";//列族名，当前人群画像
	public static final String TB_HISTORY_CF_Q1 = "labval";//列名，当前人群画像标签值，json字符串{标签值1:人数1,标签值2:人数2}
	public static final String ROWKEY_SPLITSTR = "-";//rowkey中的分隔符
	public static final String KEY_DATA_FLAG = "DATA_FLAG";//数据写入标记，1代表写入完成
	/*
	 * 分隔标签值和人数如：标签值1##人数1@;标签值2##人数2
	 * 用于合并过程中临时拼接，最终根据规则组合成json字符串
	 */
	public static final String LABEL_VALUE_SPLITSTR = "##";
	public static final String LABEL_VALUE_REDUCE_SPLITSTR = "@;";
	public static final ObjectMapper MAPPER = new ObjectMapper();
	
	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	/**
	 * 入口函数
	 * @param args
	 */
	public static void main(String[] args) {
		Map<String,String> param = getParaMap(args);
		if(param == null){
			log.info("======Please input procName");
			return;
		}
		
		SnapshotProc2 snp = new SnapshotProc2();
		String appname = param.get("appName");
		if(appname != null && !appname.equals("")){
			snp.setAppName(appname);
		}
		
		long start = System.currentTimeMillis();
		log.info("======procName:"+param.get("procName"));
		if("default".equals(param.get("procName"))){
			log.info("======doDefault ...");
			snp.doDefault(param);
		}else if("history".equals(param.get("procName"))){
			log.info("======doHistoryProc ...");
			snp.doHistoryProc(param);
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
				if(args.length != 5){
					System.err.println("Usage: SnapshotProc2 <procName> <crn> <currTime> <appName> <outPutType>");
					System.exit(1);
				}
				map.put("procName", args[0]);
				map.put("crn", args[1]);
				map.put("currTime", args[2]);
				map.put("appName", args[3]);
				map.put("outPutType", args[4]);//支持输出结果到hdfs、hbase 或者 both
			}else if("history".equals(args[0])){
				if(args.length != 5){
					System.err.println("Usage: SnapshotProc2 <procName> <crn> <begin-end> <appName> <outPutType>");
					System.exit(1);
				}
				map.put("procName", args[0]);
				map.put("crn", args[1]);
				map.put("begin-end", args[2]);
				map.put("appName", args[3]);
				map.put("outPutType", args[4]);//支持输出结果到hdfs、hbase 或者 both
			}
			return map;
		}
		return null;
	}

	/**
	 * 动态从数据库获取参与计算的配置信息
	 * @return
	 */
	protected Map<String,List<String>> loadListParam(){
		long start = System.currentTimeMillis();
		Map<String,List<String>> listParam = DBOperation.loadListParam();
		log.info("======OracleJob used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
		return listParam;
	}
	
	/**
	 * 默认处理过程
	 * 1.从oracle加载配置,从redis加载快照
	 * 2.计算归属地
	 * 3.依次关联标签进行统计汇总
	 * 4.输出基站扇区及统计结果
	 * 5.输出栅格级统计结果
	 * 6.输出区域及统计结果
	 */
	public void doDefault(Map<String,String> param){
		if(param == null){
			log.error("参数param为空");
			return;
		}
		doDefault(param,loadListParam());
	}

	/**
	 * 默认处理过程(重载方法)
	 * 1.从oracle加载配置,从redis加载快照
	 * 2.计算归属地
	 * 3.依次关联标签进行统计汇总
	 * 4.输出基站扇区及统计结果
	 * 5.输出栅格级统计结果
	 * 6.输出区域及统计结果
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
			JavaRDD<String> currData = createSnapshot(jsc, locDataList, param, PROPS.getProperty("POS_SNAPSHOT_PATH"));//持久化快照
			long start = System.currentTimeMillis();
			doDefalutCalcAndOutput(jsc,param,currData,disList);
			log.info("======doDefalutCalcAndOutput[defualt] used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
		}
		catch(Exception e){
			log.error("SnapshotProc2 error",e);
		}
		finally{
			doRelease(jsc);//释放资源
		}
	}

	
	/**
	 * 历史数据处理过程(多批次连续执行)
	 * 1.从oracle加载配置,从hdfs加载快照
	 * 2.计算归属地
	 * 3.依次关联标签进行统计汇总
	 * 4.输出基站扇区及统计结果
	 * 5.输出栅格级统计结果
	 * 6.输出区域及统计结果
	 */
	protected void doHistoryProc(Map<String,String> param){
		if(param == null){
			log.error("参数param为空");
			return;
		}
		List<String> timeRange = getTimeRange(param.get("begin-end"),"yyyyMMddHHmmss",15);
		if(timeRange == null){
			log.error("参数begin-end非法，或时间转化错误："+param.get("begin-end"));
			return;
		}
		log.info("======doHistoryProc;bacthcSize="+timeRange.size()+";timeRange=["+param.get("begin-end")+"]");
		Map<String,List<String>> disList = loadListParam();
		doHistoryProc(param,disList,timeRange);
	}
	
	/**
	 * 根据起止日期生成时间段
	 * @param begin_end
	 * @param dateformat
	 * @param duration
	 * @return
	 */
	protected static List<String> getTimeRange(String begin_end,String dateformat,int duration){
		if(begin_end == null || begin_end.equals("")){
			return null;
		}
		if(begin_end.indexOf("-") < 0){
			List<String> ls = new ArrayList<String>();
			ls.add(begin_end);
			return ls;
		}
		String[] arr = begin_end.split("-");
		String btime = arr[0];
		String etime = arr[1];
		if(btime == null || btime.equals("") || btime.length() != 14
				|| etime == null || etime.equals("") || etime.length() != 14){
			return null;
		}
		
		Calendar ca = Calendar.getInstance();
		ca.setTime(DateUtil.strToDate(btime, dateformat));
		List<String> ls = new ArrayList<String>();
		while(true){
			ls.add(btime);
			ca.add(Calendar.MINUTE, duration);//增加15分钟
			btime = DateUtil.dateToStr(ca.getTime(), dateformat);
			if(etime.compareTo(btime) < 0){
				//当开始时间大于结束时间，则跳出循环
				break;
			}
		}
		return ls;
	}
	

	/**
	 * 历史数据处理过程(重载方法)
	 * 1.从oracle加载配置,从hdfs加载快照
	 * 2.计算归属地
	 * 3.依次关联标签进行统计汇总
	 * 4.输出基站扇区及统计结果
	 * 5.输出栅格级统计结果
	 * 6.输出区域及统计结果
	 */
	protected void doHistoryProc(Map<String,String> param, Map<String,List<String>> disList,List<String> timeRange){
		if(param == null){
			log.error("参数param为空");
			return;
		}
//		List<String> gridDisList = disList.get("gridDisList");//从Oracle获取配置
		
		if(timeRange == null || timeRange.isEmpty()){
			log.error("参数timeList为空");
			return;
		}
		JavaSparkContext jsc = doInit();//初始化
		JavaRDD<String> currData = null;
		for(String currTime : timeRange){
			log.info("======doHistoryProc;currTime="+currTime);
			try{
				param.put("currTime", currTime);//设置当前日期
				/*
				 * 获取快照
				 * 73CD34AC44E37A62982440ECD8C3A67B,lac$ci,1860172,20170101120000
				 * E441B62395FE07A0A83E6B6725EA62D3,lac$ci,1860172,20170101120000
				 * 17BF5832F7A0CFBC40C8772821E1A97C,lac$ci,1860172,20170101120000
				 */
				currData = jsc.textFile(PROPS.getProperty("POS_SNAPSHOT_PATH") + param.get("currTime") + "/");
				long start = System.currentTimeMillis();
				doDefalutCalcAndOutput(jsc,param,currData,disList);
				log.info("======doDefalutCalcAndOutput[History] used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
			}
			catch(Exception e){
				log.error("SnapshotProc2 History error",e);
			}
		}
		doRelease(jsc);//释放资源
	}


	/**
	 * 默认计算过程以及输出
	 * @param jsc
	 * @param param
	 * @param currData
	 * @param
	 * @param
	 * @throws Exception
	 */
	protected void doDefalutCalcAndOutput(JavaSparkContext jsc,Map<String,String> param,JavaRDD<String> currData,Map<String,List<String>> disList) throws Exception{
		if(currData == null || currData.isEmpty()){
			log.error("======currData is empty");
			return;
		}
		List<String> gridDisList = disList.get("gridDisList");//从Oracle获取配置
		List<String> fanDisList = disList.get("fanDisList");//从Oracle获取配置
		List<String> disDisList = disList.get("disDisList");//从Oracle获取配置
		

		/*
		 * 统计号段归属地以及总人数。输出格式如下：
		 * key=基站扇区,标签名,标签值		value=人数
		 * 样例：
		 * key=lac$ci,uv,uv			value=100
		 * key=lac$ci,ufrom,310000	value=50
		 * key=lac$ci,ufrom,320000	value=50
		 */
		JavaRDD<String> uattrData = jsc.textFile(PROPS.getProperty("UFROM_HDFS_PATH"));// 从hdfs读取号段归属地信息
		JavaPairRDD<String, Integer> rsPeopleOrg = calcPeopleOrg(currData, uattrData);
		
		/*
		 * 根据标签文件数量，依次统计结果。输出格式如下：
		 * key=基站扇区,标签名,标签值		value=人数
		 * 样例：
		 * key=lac$ci,age,25岁以下	value=100
		 * key=lac$ci,age,26-29岁	value=50
		 * key=lac$ci,gender,男		value=50
		 * key=lac$ci,gender,女		value=50
		 * ...
		 */
		log.info("根据标签文件数量，依次关联客户标签并统计结果");
		JavaPairRDD<String, String> currData2 = convertPairData(currData);
		List<JavaPairRDD<String, Integer>> rsLabelList = calcLabel(jsc, currData2, PROPS);
		
		//求和后,合并结果集
		JavaPairRDD<String, Integer> resultFan = reduceResult(rsPeopleOrg);
		for(JavaPairRDD<String, Integer> rsLabel : rsLabelList){
			resultFan = resultFan.union(reduceResult(rsLabel));
		}
		
		/*
		 * 读取扇区与栅格的人数统计比例：
		 * 基站扇区,栅格id,人数统计比例
		 * 样例：
		 * sitefan,gridid,sratio
		 * lac$ci1,gridA,0.5
		 * lac$ci1,gridB,0.5
		 */
		JavaRDD<String> fanGrid = jsc.textFile(PROPS.getProperty("FANS_GRID_HDFS_PATH"));
		/*
		 * 计算栅格级别的统计。输出格式如下：
		 * key=栅格id,标签名,标签值		value=人数
		 * 样例：
		 * key=grid001,uv,uv		value=100
		 * key=grid001,ufrom,310000	value=50
		 * key=grid001,ufrom,320000	value=50
		 * key=grid001,gender,男		value=50
		 * key=grid001,gender,女		value=50
		 */
		JavaPairRDD<String, Integer> resultGrid = aggregateResultToHighLevel(resultFan,fanGrid);
		
		//1.热点区域统计(基于栅格的标准区域汇聚)
		JavaPairRDD<String, Integer> resultSub = null;
		if(gridDisList != null && !gridDisList.isEmpty()
				&& resultGrid != null && !resultGrid.isEmpty()){
			/*
			 * 计算区域级别的统计。输出格式如下：
			 * key=区域id,标签名,标签值		value=人数
			 * 样例：
			 * key=dis001,uv,uv		value=100
			 * key=dis001,ufrom,310000	value=50
			 * key=dis001,ufrom,320000	value=50
			 * key=dis001,gender,男		value=50
			 * key=dis001,gender,女		value=50
			 */
			resultSub = aggregateResultToHighLevel(resultGrid,jsc.parallelize(gridDisList).cache());
			if(resultSub != null && !resultSub.isEmpty()){
				resultSub = resultSub.union(reduceAll(resultSub));//全市
			}
		}
		
		//2.热点区域统计(基于基站扇区的非标准区域汇聚)
		JavaPairRDD<String, Integer> resultSub2 = null;
		if(fanDisList != null && !fanDisList.isEmpty()
				&& resultFan != null && !resultFan.isEmpty()){
			resultSub2 = aggregateResultToHighLevel(resultFan,jsc.parallelize(fanDisList).cache());
			resultSub = (resultSub != null && !resultSub.isEmpty()) ? resultSub.union(resultSub2) : resultSub2;
		}
		
		//3.热点区域统计(基于非标准区域的区域汇聚)
		if(disDisList != null && !disDisList.isEmpty() 
				&& resultSub2 != null && !resultSub2.isEmpty()){
			JavaPairRDD<String, Integer> resultSub3 = aggregateResultToHighLevel(resultSub2,jsc.parallelize(disDisList).cache());
			if(resultSub3 != null && !resultSub3.isEmpty()){
				resultSub = (resultSub != null && !resultSub.isEmpty()) ? resultSub.union(resultSub3) : resultSub3;
			}
		}
		
		//输出结果
		doOutPut(param, resultFan, resultGrid, resultSub);
		
	}
	
	/**
	 * 输出结果
	 * @param param
	 * @param resultFan
	 * @param resultGrid
	 * @param resultSub
	 */
	protected void doOutPut(Map<String,String> param,JavaPairRDD<String, Integer> resultFan,
			JavaPairRDD<String, Integer> resultGrid,JavaPairRDD<String, Integer> resultSub){
		//计算结束输出结果
		String outPutType = param.get("outPutType");
		long start = System.currentTimeMillis();
		if("both".equals(outPutType) || "hdfs".equals(outPutType)){
			log.info("输出结果到hdfs");
//			writeToHdfs(resultFan,PROPS.getProperty("POS_STAT_PATH") + param.get("currTime") + "/" + DIR_STAT_FAN);
//			log.info("======writeToHdfs(resultFan) used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
//			start = System.currentTimeMillis();
			writeToHdfs(resultGrid,PROPS.getProperty("POS_STAT_PATH") + param.get("currTime") + "/" + DIR_STAT_GRID);
			log.info("======writeToHdfs(resultGrid) used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
			start = System.currentTimeMillis();
			writeToHdfs(resultSub,PROPS.getProperty("POS_STAT_PATH") + param.get("currTime") + "/" + DIR_STAT_SUB);
			log.info("======writeToHdfs(resultSub) used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
		}
		start = System.currentTimeMillis();
		if("both".equals(outPutType) || "hbase".equals(outPutType)) {
			log.info("输出结果到hbase");
//			writeToHBase(resultFan, param);
//			log.info("======writeToHBase(resultFan) used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
//			start = System.currentTimeMillis();
			writeToHBase(resultGrid, param);
			log.info("======writeToHBase(resultGrid) used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
			start = System.currentTimeMillis();
			writeToHBase(resultSub, param);
			log.info("======writeToHBase(resultSub) used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
		}
	}
	
	
	/**
	 * 初始化spark环境
	 */
	protected JavaSparkContext doInit(){
		SparkConf conf = new SparkConf().setAppName(this.getAppName());
		return new JavaSparkContext(conf);
	}
	
	/**
	 * 释放资源
	 */
	protected void doRelease(JavaSparkContext jsc){
		if(jsc != null){
			jsc.stop();
		}
	}
	
	/**
	 * 获取redis位置快照
	 * @param param
	 * @return
	 */
	protected List<String> getLocData(Map<String,String> param){
		String crn = param.get("crn");
		log.info("crn======"+crn);
		//从redis获取实时位置数据
		log.info("从redis读取实时位置数据locData1");
		List<String> locData = RedisParallel3.loadKVs(EntityUtil.REDIS_USER_LOCATION_KEY+"*", crn);
		log.info("redis加载数据完成");
		return locData;
	}
	
	/**
	 * 创建快照
	 * @param param
	 * @param locData
	 * @return
	 */
	protected JavaRDD<String> createSnapshot(JavaSparkContext sc, List<String> locData, 
			Map<String,String> param, String savePath){
		JavaRDD<String> returnData = procSnapshot(sc, locData, param);
		//持久化完成后返回
//		returnData.coalesce(1).saveAsTextFile(savePath + currTime);//存储当前快照文件到hdfs
		writeSnapshot2hdfs(returnData, param, savePath);//存储当前快照文件到hdfs
		return returnData;
	}

	/**
	 * 处理快照
	 * @param param
	 * @param locData
	 * @return
	 */
	private JavaRDD<String> procSnapshot(JavaSparkContext sc, List<String> locData, 
			Map<String,String> param){
		final String currTime = param.get("currTime");
		log.info("currTime======"+currTime);
		JavaRDD<String> currData = sc.parallelize(locData).cache();//将redis的结果集转化成rdd
		JavaRDD<String> returnData = currData.map(new Function<String, String>(){
			@Override
			public String call(String v1) throws Exception {
				String[] msg = v1.split(EntityUtil.DB_RESULT_SPLITSTR);
				if(msg == null || msg.length != 2){
					return "";
				}
				String msisdn7 = (msg[0] != null && msg[0].length() > 7) ?  msg[0].substring(0, 7) : msg[0]; 
				return MD5EncryptTools.str2MD5Encrypt(msg[0]) 
						+ EntityUtil.DB_RESULT_SPLITSTR + msg[1] 
						+ EntityUtil.DB_RESULT_SPLITSTR + msisdn7
						+ EntityUtil.DB_RESULT_SPLITSTR + currTime;
			}
		});
		
		return returnData;
	}
	
	/**
	 * 持久化快照
	 * @param
	 * @param param
	 * @param savePath
	 */
	protected void writeSnapshot2hdfs(JavaRDD<String> snapShotData,Map<String,String> param, String savePath){
		String currTime = param.get("currTime");
		snapShotData.coalesce(1).saveAsTextFile(savePath + currTime);//存储当前快照文件到hdfs
	}

	protected JavaPairRDD<String, Integer> calcPeopleOrg(JavaRDD<String> currData,
			JavaRDD<String> uattrData){
		log.info("将currData转化成JavaPairRDD;key=号码前7位");//key=号段，value=基站扇区
		JavaPairRDD<String, String> currDataPair = currData.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] arr = arg0.split(EntityUtil.DB_RESULT_SPLITSTR);
//				return new Tuple2<String, String>(arr[0].substring(0, 7), arr[1]);
				return new Tuple2<String, String>(arr[2], arr[1]);  //arr[2] 手机号前7位，arr[1]: 基站扇区lac$ci
			}
		});
		return calcPeopleOrg(currDataPair,uattrData);
	}
	
	/**
	 * 计算归属地及总人数
	 * 
	 * @param
	 */
	protected JavaPairRDD<String, Integer> calcPeopleOrg(JavaPairRDD<String, String> currDataPair,
			JavaRDD<String> uattrData){
		
		//key=号段，value=行政区
		JavaPairRDD<String, String> uattrDataPair = uattrData.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] arr = arg0.split(SPLITSTR_UFROM,SPLIT_LIMIT_UFROM);
				return new Tuple2<String, String>(arr[0], arr[1]);
			}
		});
		
		log.info("将位置数据currDataPair与号段进行左连接");
		JavaPairRDD<String, Tuple2<String, org.apache.spark.api.java.Optional<String>>> joinDataPair1 = currDataPair.leftOuterJoin(uattrDataPair);
//		for (Tuple2<String, Tuple2<String, Optional<String>>> s : joinDataPair1.collect()) {
//			log.info(s._1()+";"+s._2()._1()+";"+(s._2()._2().isPresent() ? s._2()._2().get() : null));
//			log.info(s._2()._1());// 左表数据
//			log.info(s._2()._2().isPresent() ? s._2()._2().get() : null);// 右表数据
//		}
		
		log.info("将连接后的数据进行扁平化");//key=基站扇区,ufrom,行政区划,人数1
		JavaPairRDD<String, Integer> rs1 = joinDataPair1.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<String, org.apache.spark.api.java.Optional<String>>>, String, Integer>() {
			@Override
			public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Tuple2<String, org.apache.spark.api.java.Optional<String>>> arg0) throws Exception {
				String tabLeft = arg0._2()._1();// 左表数据:基站扇区
				String tabRigth = arg0._2()._2().isPresent() ? arg0._2()._2().get() : null;// 右表数据:行政区划

				List<Tuple2<String, Integer>> returnVal = new ArrayList<Tuple2<String, Integer>>();// 返回值
				if(tabLeft == null || tabLeft.equals("") || tabLeft.equals(EntityUtil.SITE_FAN_SPLIT)){
					log.error("======calcPeopleOrg;key="+arg0._1()+";tabLeft="+tabLeft+";tabRigth="+tabRigth);
					tabLeft = EntityUtil.UNKOWNSTR;
				}
				//归属地
				if(tabRigth == null || tabRigth.equals("")){
					returnVal.add(new Tuple2<String, Integer>(tabLeft + EntityUtil.DB_RESULT_SPLITSTR + UFROM + EntityUtil.DB_RESULT_SPLITSTR + EntityUtil.UNKOWNSTR, 1));
					returnVal.add(new Tuple2<String, Integer>(tabLeft + EntityUtil.DB_RESULT_SPLITSTR + UFROM_SHORT + EntityUtil.DB_RESULT_SPLITSTR + EntityUtil.UNKOWNSTR, 1));
				}else{
					if(UFROM_SHORT_VALUE_LOCAL.equals(tabRigth)){
						returnVal.add(new Tuple2<String, Integer>(tabLeft + EntityUtil.DB_RESULT_SPLITSTR + UFROM_SHORT + EntityUtil.DB_RESULT_SPLITSTR + tabRigth, 1));
					} else{
						//如果行政区划不为上海，则认为是外省
						returnVal.add(new Tuple2<String, Integer>(tabLeft + EntityUtil.DB_RESULT_SPLITSTR + UFROM_SHORT + EntityUtil.DB_RESULT_SPLITSTR + UFROM_SHORT_VALUE_NON_LOCAL, 1));
					}
					returnVal.add(new Tuple2<String, Integer>(tabLeft + EntityUtil.DB_RESULT_SPLITSTR + UFROM + EntityUtil.DB_RESULT_SPLITSTR + tabRigth, 1));
				}
				//总人数
				returnVal.add(new Tuple2<String, Integer>(tabLeft + EntityUtil.DB_RESULT_SPLITSTR + SUMSTR + EntityUtil.DB_RESULT_SPLITSTR + SUMSTR, 1));// GRID01,UV,UV
				return returnVal.iterator();

			}
		});

		return rs1;
	}
	
	/**
	 * 根据标签文件个数，依次计算单个标签
	 * @param sc
	 * @param
	 * @param labelProps
	 * @return
	 */
	protected  List<JavaPairRDD<String, Integer>> calcLabel(JavaSparkContext sc,
			JavaPairRDD<String, String> currData2,Properties labelProps){
		int num = Integer.parseInt(labelProps.getProperty("LABLE_FILE_NUM"));//总大小
		String[] r_cols = null;
		String[] r_cols_index = null;
		String split_lablel = null;
		List<JavaPairRDD<String, Integer>> list = new ArrayList<JavaPairRDD<String, Integer>>();
		for(int i=1; i<=num; i++){
			r_cols = labelProps.getProperty("LABLE_FILE."+i+".R_COLS").split(",");
			r_cols_index = labelProps.getProperty("LABLE_FILE."+i+".R_COLS_INDEX").split(",");
			split_lablel = SPLITSTR_LABEL[Integer.parseInt(PROPS.getProperty("LABLE_FILE."+i+".SPLITSTR_INDEX"))];
			list.add(calcLabel(currData2, sc.textFile(PROPS.getProperty("LABLE_FILE."+i+".HDFS_PATH")),
					r_cols, r_cols_index, split_lablel,
					Integer.parseInt(PROPS.getProperty("LABLE_FILE."+i+".R_COLS_LENGTH"))));
		}
		return list;
	}
	
	/**
	 * 将当前快照数据转化成key-value结构
	 * @param currData
	 * @return
	 */
	private  JavaPairRDD<String, String> convertPairData(JavaRDD<String> currData){
		log.info("将currData转化成JavaPairRDD;key=号码");//key=号码;value=基站扇区
		JavaPairRDD<String, String> currData2 = currData.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] arr = arg0.split(EntityUtil.DB_RESULT_SPLITSTR);
				return new Tuple2<String, String>(arr[0], arr[1]);
			}
		});
		return currData2;
	}
	
	
	/**
	 * 计算单个标签
	 * @param
	 * @param baseLabel 基础标签数据
	 * @param r_cols 字段名
	 * @param r_cols_index 字段在标签数据中的位置
	 * @param split_lablel 标签数据分隔符
	 * @param r_cols_length 标签数据字段总数
	 * @return
	 */
	private  JavaPairRDD<String, Integer> calcLabel(JavaPairRDD<String, String> currData2,JavaRDD<String> baseLabel,
			final String[] r_cols, final String[] r_cols_index, final String split_lablel,final int r_cols_length){
		log.info("将baseLable转化成JavaPairRDD");//key=号码;value=年龄,性别,arpu
		JavaPairRDD<String, String> ckmDataPair = baseLabel.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				return new Tuple2<String, String>(arg0.split(split_lablel)[0], arg0);
			}
		});

		log.info("将位置数据currData2与客户标签ckmDataPair进行左连接");
		JavaPairRDD<String, Tuple2<String, org.apache.spark.api.java.Optional<String>>> joinDataPair2 = currData2.leftOuterJoin(ckmDataPair);
//		for (Tuple2<String, Tuple2<String, Optional<String>>> s : joinDataPair2.collect()) {
//			log.info(s._1()+";"+s._2()._1()+";"+(s._2()._2().isPresent() ? s._2()._2().get() : null));
//			log.info(s._2()._1());// 左表数据
//			log.info(s._2()._2().isPresent() ? s._2()._2().get() : null);// 右表数据
//		}
		log.info("将连接后的数据joinDataPair进行扁平化，重新组合key，为统计做准备");
		JavaPairRDD<String, Integer> rs2 = joinDataPair2.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Tuple2<String, org.apache.spark.api.java.Optional<String>>>, String, Integer>() {
					@Override
					public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Tuple2<String, org.apache.spark.api.java.Optional<String>>> arg0)
							throws Exception {
						String tabLeft = arg0._2()._1();// 左表数据:基站扇区
						String tabRigth = arg0._2()._2().isPresent() ? arg0._2()._2().get() : null;// 右表数据:号码,年龄,性别,arpu

						List<Tuple2<String, Integer>> returnVal = new ArrayList<Tuple2<String, Integer>>();// 返回值
						if(tabLeft == null || tabLeft.equals("") || tabLeft.equals(EntityUtil.SITE_FAN_SPLIT)){
							log.error("======calcLabel-1;key="+arg0._1()+";tabLeft="+tabLeft+";tabRigth="+tabRigth);
							tabLeft = EntityUtil.UNKOWNSTR;
						}

						/* 右表数据加工 */
						if (tabRigth == null) {
							// 如果右表无数据，则可能是非上海联通用户，将属性标签设置为未知
							// 如性别的key=123$456,sex,空值
//							log.error("======calcLabel-2;key="+arg0._1()+";tabLeft="+tabLeft+";tabRigth="+tabRigth);
//							for (int i = 0; i < r_cols.length; i++) {
//								returnVal.add(new Tuple2<String, Integer>(
//										tabLeft + EntityUtil.DB_RESULT_SPLITSTR + r_cols[i] + EntityUtil.DB_RESULT_SPLITSTR + EntityUtil.UNKOWNSTR, 1));
//							}
						} else {
							// 如性别的key为=123$456,sex,男
							String[] arrR = null;
							try{
								arrR = tabRigth.split(split_lablel,r_cols_length);
								for (int i = 0; i < r_cols.length; i++) {
									String val = arrR[Integer.parseInt(r_cols_index[i])];
									if (val == null || val.equals("")) {
										returnVal.add(new Tuple2<String, Integer>(
												tabLeft + EntityUtil.DB_RESULT_SPLITSTR + r_cols[i] + EntityUtil.DB_RESULT_SPLITSTR + EntityUtil.UNKOWNSTR, 1));
									} else {
										returnVal.add(new Tuple2<String, Integer>(
												tabLeft + EntityUtil.DB_RESULT_SPLITSTR + r_cols[i] + EntityUtil.DB_RESULT_SPLITSTR + val, 1));
									}
								}
							}
							catch(Exception e){
								log.error("======tabRigth="+tabRigth,e);
							}
						}
						return returnVal.iterator();
					}
				});
		return rs2;
	}
	
	/**
	 * 汇聚求和，转化成最终保存的文本格式
	 * @param result
	 * @return
	 */
	protected  JavaPairRDD<String, Integer> reduceResult(JavaPairRDD<String, Integer> result){
		log.info("针对各属性指标进行统计");
		JavaPairRDD<String, Integer> rs = result.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}});
		return rs;
	}
	
	/**
	 * 输出结果到hdfs
	 * @param result
	 * @param savePath
	 * @return
	 */
	public boolean writeToHdfs(JavaPairRDD<String, Integer> result,String savePath){
		if(result == null || result.isEmpty()){
			log.error("======result is empty [writeToHdfs] ");
			return false;
		}
		//将rdd转换成String类型方便写文件
		JavaRDD<String> rs = result.map(new Function<Tuple2<String,Integer>,String>(){
			@Override
			public String call(Tuple2<String, Integer> v1) throws Exception {
				return v1._1() + EntityUtil.DB_RESULT_SPLITSTR + v1._2();
			}
		});
		rs.coalesce(1).saveAsTextFile(savePath);
		return true;
	}
	

	/**
	 * 将单个字符串转化成json对象字符串
	 * 
	 * key=基站扇区,标签名;value=标签值1##人数1@;标签值2##人数2@;标签值3##人数3
	 * 转化成
	 * json字符串{标签值1:人数1,标签值2:人数2,标签值2:人数2}
	 * 
	 * @param sigleString
	 * @return
	 */
	public static String convertToJson(String sigleString){
		if(sigleString == null || sigleString.equals("")){
			return null;
		}
		String[] labels = sigleString.split(LABEL_VALUE_REDUCE_SPLITSTR);
		if(labels == null || labels.length <= 0){
			return null;
		}
		String label = null;
		String[] arr_label = null;
		Map<String,Object> returnMap = new HashMap<String,Object>();
		//逐个标签进行解析
		for(int i = 0; i < labels.length; i++){
			label = labels[i];
			if(label == null || label.equals("")){
				continue;
			}
			arr_label = label.split(LABEL_VALUE_SPLITSTR);
			if(arr_label == null || arr_label.length != 2){
				continue;
			}
			try {
				returnMap.put(arr_label[0], Integer.valueOf(arr_label[1]));
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
		if(returnMap.isEmpty()){
			return null;
		}
		try {
			return MAPPER.writeValueAsString(returnMap);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 将结果写入hbase
	 * @param result
	 * @param param
	 */
	protected boolean writeToHBase(JavaPairRDD<String, Integer> result, Map<String,String> param){
		if(result == null || result.isEmpty()){
			log.error("======result is empty [writeToHBase] ");
			return false;
		}
		final String currTime = param.get("currTime");
		List<Tuple2<String, String>>  rs = prepareRDDForHBase(result).collect();
		log.info("rs.size()======"+rs.size());
	
		if(rs == null || rs.isEmpty()){
			return false;
		}
		doWriteHBase(rs, currTime, TB_HISTORY, TB_HISTORY_CF, TB_HISTORY_CF_Q1);
		return true;
	}
	
	/**
	 * 将结果批量写入HBase
	 * @param rs
	 * @param currTime
	 * @param hbase_tab
	 * @param hbase_cf
	 * @param hbase_q
	 */
	protected void doWriteHBase(List<Tuple2<String, String>> rs,String currTime,
			String hbase_tab,String hbase_cf,String hbase_q){
		//将结果写入hbase
		log.info("=============connect hbase ...");
		Configuration conf = HBaseConfiguration.create();
		//conf.set("hbase.zookeeper.quorum", PROPS.getProperty("hbase.zookeeper.quorum"));
		//conf.set("hbase.zookeeper.property.clientPort", PROPS.getProperty("hbase.zookeeper.property.clientPort"));

		System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("hbase.security.authentication","kerberos");
		conf.set("keytab.file" , "/home/beho/.key/beho.keytab");
		conf.set("kerberos.principal", "beho@BONC.COM");

		conf.set("kerberos.principal" , "beho@BONC.COM" );
		conf.set("hbase.master.kerberos.principal", "hbase/_HOST@BONC.COM");
		conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@BONC.COM");
		conf.set("hbase.zookeeper.quorum","bo-hadoop001.bonc.com,bo-hadoop002.bonc.com,bo-hadoop003.bonc.com");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		Connection connection = null;
		Table table = null;
		try {
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab("beho@BONC.COM","/home/beho/.key/beho.keytab");

			Connection conn = ConnectionFactory.createConnection(conf);
			Admin admin = conn.getAdmin();
			connection = ConnectionFactory.createConnection(conf);
			log.info("=============hbase connected ...");
			table = connection.getTable(TableName.valueOf(hbase_tab));
			List<Put> puts = new ArrayList<Put>();
			Put put = null;
			String rowid = null;
			String values = null;
			for(Tuple2<String,String> obj : rs){
				values = convertToJson(obj._2());
				if(values == null || values.equals("")){
					continue;//异常情况则不写数据
				}
				if(currTime != null && !currTime.equals("")){
					rowid = obj._1() + ROWKEY_SPLITSTR + currTime;//行键
				}
				else{
					rowid = obj._1();
				}
				put = new Put(Bytes.toBytes(rowid));
				put.addColumn(Bytes.toBytes(hbase_cf), Bytes.toBytes(hbase_q), Bytes.toBytes(values));
				puts.add(put);
			}
			table.put(puts);
			//数据批量写入标记位，仅针对历史数据写入
			if(currTime != null && !currTime.equals("")){
				log.info("=============updateflag ...");
				rowid = KEY_DATA_FLAG + ROWKEY_SPLITSTR + currTime;//行键
				put = new Put(Bytes.toBytes(rowid));
				put.addColumn(Bytes.toBytes(hbase_cf), Bytes.toBytes(hbase_q), Bytes.toBytes("1"));
				table.put(put);
			}
			
		 }catch(Exception e){
			 e.printStackTrace();
		 }finally {
			try {
				table.close();
				connection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			log.info("=============hbase closed ...");
		 }
	}
	
	
	/**
	 * 根据hbase结构组合key
	 * @param
	 * @return
	 */
	protected JavaPairRDD<String, String> prepareRDDForHBase(JavaPairRDD<String, Integer> result){
		/*
		 * 改变key-value格式，用于后续合并
		 * (key=基站扇区,标签名,标签值;value=人数) 转化成  (key=基站扇区,标签名;value=标签值##人数 )
		 */
		JavaPairRDD<String, String> rs1 = result.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, Integer> arg0) throws Exception {
				String[] arr = arg0._1().split(EntityUtil.DB_RESULT_SPLITSTR);
				return new Tuple2<String, String>(arr[0] + ROWKEY_SPLITSTR + arr[1], 
						arr[2] + LABEL_VALUE_SPLITSTR + arg0._2());
			}
		});
		/*
		 * 根据key进行合并
		 * 多条(key=基站扇区,标签名;value=标签值##人数 )
		 * 合并成  
		 * 单条(key=基站扇区,标签名;value=标签值1##人数1@;标签值2##人数2@;标签值3##人数3)
		 */
		JavaPairRDD<String, String> rs = rs1.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String v1, String v2) throws Exception {
				return v1 + LABEL_VALUE_REDUCE_SPLITSTR + v2;
			}});
		return rs;
	}

	
	/**
	 * 汇聚统计结果到更高级别
	 * @param resultFan
	 * @param fanGrid
	 * @return
	 */
	private JavaPairRDD<String, Integer> aggregateResultToHighLevel(JavaPairRDD<String, Integer> resultFan, JavaRDD<String> fanGrid){
		/*
		 * 改变key-value格式，用于后续连接
		 * (key=基站扇区,标签名,标签值;value=人数) 转化成  (key=基站扇区;value=标签名,标签值,人数 )
		 * or
		 * (key=栅格id,标签名,标签值;value=人数) 转化成  (key=栅格id;value=标签名,标签值,人数 )
		 */
		JavaPairRDD<String, String> currDataPair = resultFan
				.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, Integer> arg0) throws Exception {
				String[] arr = arg0._1().split(EntityUtil.DB_RESULT_SPLITSTR);
				return new Tuple2<String, String>(arr[0], arr[1] + EntityUtil.DB_RESULT_SPLITSTR 
						+ arr[2] + EntityUtil.DB_RESULT_SPLITSTR + arg0._2());
			}
		});
		
		/*
		 * 基站扇区,栅格id,人数统计占比	==>	key=基站扇区;value=栅格id,人数统计占比
		 * or
		 * 栅格id,区域id	==>	key=栅格id;value=区域id,人数统计占比(1)
		 */
		JavaPairRDD<String, String> fanGridPair = fanGrid.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] arr = arg0.split(EntityUtil.DB_RESULT_SPLITSTR);
				if(arr.length == 3){
					//sitefan ==> grid
					return new Tuple2<String, String>(arr[0], arr[1] + EntityUtil.DB_RESULT_SPLITSTR + arr[2]);
				}else{
					//grid ==> dis
					return new Tuple2<String, String>(arr[0], arr[1] + EntityUtil.DB_RESULT_SPLITSTR + "1");
				}
			}
		});
		
		//两个集合进行内连接
		JavaPairRDD<String, Tuple2<String, String>> joinDataPair1 = currDataPair.join(fanGridPair);
//		for (Tuple2<String, Tuple2<String, String>> s : joinDataPair1.collect()) {
//			log.info(s._2()._1());// 左表数据
//			log.info(s._2()._2());// 右表数据
//		}
		/*
		 * 将连接后的数据进行扁平化
		 * key=栅格ID,标签名,标签值;value=人数
		 * or
		 * key=区域ID,标签名,标签值;value=人数
		 */
		JavaPairRDD<String, Double> rs1 = joinDataPair1.flatMapToPair(
			new PairFlatMapFunction<Tuple2<String, Tuple2<String, String>>, String, Double>() {

				@Override
				public Iterator<Tuple2<String, Double>> call(Tuple2<String, Tuple2<String, String>> arg0)
						throws Exception {
					String tabLeft = arg0._2()._1();// 左表数据:key=基站扇区;		value=标签名,标签值,人数
					String tabRight = arg0._2()._2();// 右表数据:key=基站扇区;	value=栅格id,人数统计占比
					String[] arrLeft = tabLeft.split(EntityUtil.DB_RESULT_SPLITSTR);
					String[] arrRight = tabRight.split(EntityUtil.DB_RESULT_SPLITSTR);
					
					//key值获取
					String key = null;
					if(tabRight == null || arrRight[0] == null || arrRight[0].equals("")){
						key = EntityUtil.UNKOWNSTR + EntityUtil.DB_RESULT_SPLITSTR + arrLeft[0] + EntityUtil.DB_RESULT_SPLITSTR + arrLeft[1];
					}else{
						key = arrRight[0] + EntityUtil.DB_RESULT_SPLITSTR + arrLeft[0] + EntityUtil.DB_RESULT_SPLITSTR + arrLeft[1];
					}
					//人数计算
					double pcnt = Double.parseDouble(arrLeft[2]);//默认人数
					if(tabRight != null && arrRight.length == 2
							&& arrRight[1] != null && !arrRight[1].equals("")){
						pcnt = pcnt * Double.parseDouble(arrRight[1]);
					}

					List<Tuple2<String, Double>> returnVal = new ArrayList<Tuple2<String, Double>>();// 返回值
					returnVal.add(new Tuple2<String, Double>(key , pcnt));
					return returnVal.iterator();
				}
				
			});
			//根据key进行求和累计
			JavaPairRDD<String, Double> rs2 = rs1.reduceByKey(new Function2<Double, Double, Double>() {
				@Override
				public Double call(Double v1, Double v2) throws Exception {
					return v1 + v2;
				}
			});
			
			//降小数转化成整数
			JavaPairRDD<String, Integer> rs = rs2.mapToPair(new PairFunction<Tuple2<String, Double>, String, Integer>(){
				@Override
				public Tuple2<String, Integer> call(Tuple2<String, Double> t) throws Exception {
					return new Tuple2<String, Integer>(t._1() , (int) Math.round(t._2().doubleValue()));
				}
			});
			
			
		return rs;
	}
	
	/**
	 * 计算全局
	 * @param resultSub
	 * @param
	 * @return
	 */
	private static JavaPairRDD<String, Integer> reduceAll(JavaPairRDD<String, Integer> resultSub){
		//根据key前缀过滤
		JavaPairRDD<String, Integer> rs1 = resultSub.filter(new Function<Tuple2<String, Integer>,Boolean>(){
			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				String key = v1._1();
				if(key != null && key.startsWith(PRE_STR_CANTON)){
					//以CAN前缀开头的都会全市的行政区划，后续根据行政区划求和得到全市范围画像
					return true;
				}
				return false;
			}
		});
		
		//将所有的
		JavaPairRDD<String, Integer> rs2 = rs1.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>(){
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> t) throws Exception {
				//key=CAN_310110,uv,uv
				String[] arr = t._1().split(EntityUtil.DB_RESULT_SPLITSTR);
				
				//key=DIS_ALL,uv,uv
				String key_new = t._1().replace(arr[0], KEY_DIS_ALL);
				return new Tuple2<String, Integer>(key_new, t._2());
			}
		});
		
		//根据key_new汇总
		JavaPairRDD<String, Integer> rs3 = rs2.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		return rs3;
		
	}

}
