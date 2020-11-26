package com.bigdata.spark;

import com.shunicom.jdbc.DBOperation;
import com.shunicom.util.DateUtil;
import com.shunicom.util.EntityUtil;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * 人群流入、流出分析类
 * 当前和历史两份快照数据，计算驻留人群、进入人群、离开人群的画像
 * 
 * @author marq7
 *
 */
public class MovementProc extends SnapshotProc2{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger log = Logger.getLogger(MovementProc.class);
	public static final String TB_HISTORY = "beho:tb_shurta_result_history";//历史快照计算结果存放表名
	public static final String TB_HISTORY_CF_CURRENT = "current";//列族名，当前人群画像
	public static final String TB_HISTORY_CF_15_MIN_AGO = "his_15min_ago";//列族名，当前人群画像
	public static final String TB_HISTORY_CF_60_MIN_AGO = "his_60min_ago";//列族名，当前人群画像
	public static final String TB_HISTORY_Q_CURRENT = "labval_current";//列名，当前人群画像标签值，json字符串{标签值1:人数1,标签值2:人数2}
	public static final String TB_HISTORY_Q_STOP = "labval_stop";//列名，当前人群画像标签值，json字符串{标签值1:人数1,标签值2:人数2}


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
		
		SnapshotProc2 snp = new MovementProc();
		String appname = param.get("appName");
		if(appname != null && !appname.equals("")){
			snp.setAppName(appname);
		}else{
			snp.setAppName("MovementProc");
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
		log.info("======"+snp.getAppName()+";used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
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
					System.err.println("Usage: MovementProc <procName> <crn> <currTime> <appName> <outPutType>");
					System.exit(1);
				}
				map.put("procName", args[0]);
				map.put("crn", args[1]);
				map.put("currTime", args[2]);
				map.put("appName", args[3]);
				map.put("outPutType", args[4]);//支持输出结果到hdfs、hbase 或者 both
			}else if("history".equals(args[0])){
				if(args.length != 5){
					System.err.println("Usage: MovementProc <procName> <crn> <begin-end> <appName> <outPutType>");
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
		Map<String,List<String>> listParam = DBOperation.loadListParamHistory();
		log.info("======OracleJob used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
		return listParam;
	}
	
	/**
	 * 默认处理过程(重载方法)
	 * 1.从oracle加载配置
	 * 2.从hdfs加载快照(当前)，关联区域位置
	 * 3.计算当前人群画像
	 * 4.从hdfs加载快照(历史)，关联区域位置
	 * 5.获取驻留人群快照，计算人群画像
	 * 6.获取进入人群快照，计算人群画像
	 * 7.后去离开人群快照，计算人群画像
	 */
	protected void doHistoryProc(Map<String,String> param, Map<String,List<String>> disList,List<String> timeRange){
		if(param == null){
			log.error("参数param为空");
			return;
		}
		if(timeRange == null || timeRange.isEmpty()){
			log.error("参数timeList为空");
			return;
		}
		for(String currTime : timeRange){
			log.info("======doHistoryProc;currTime="+currTime);
			try{
				param.put("currTime", currTime);//设置当前日期
				doDefault(param,disList);
			}
			catch(Exception e){
				log.error("MovementProc History error",e);
			}
		}
	}
	
	/**
	 * 默认处理过程(重载方法)
	 * 1.从oracle加载配置
	 * 2.从hdfs加载快照(当前)，关联区域位置
	 * 3.计算当前人群画像
	 * 4.从hdfs加载快照(历史)，关联区域位置
	 * 5.获取驻留人群快照，计算人群画像
	 * 6.获取进入人群快照，计算人群画像
	 * 7.后去离开人群快照，计算人群画像
	 */
	protected void doDefault(Map<String,String> param, Map<String,List<String>> disList){
		if(param == null){
			log.error("参数param为空");
			return;
		}
		String dateformat = "yyyyMMddHHmmss";
		String currTime = param.get("currTime");
		String currTime_15min_ago = getTimeAgo(currTime,dateformat,15);
		String currTime_60min_ago = getTimeAgo(currTime,dateformat,60);
		List<String> fanDisList = disList.get("fanDisList");//从Oracle获取配置
		JavaSparkContext jsc = doInit();//初始化
		try{
			/*
			 * 基站扇区与区域关系
			 * 
			 * key=lac$ci
			 * value=DIS_10000011
			 */
			JavaPairRDD<String, String> fanDisDataPair = getFanDisDataPair(jsc,fanDisList);
			/*
			 * 获取快照
			 * 73CD34AC44E37A62982440ECD8C3A67B,lac$ci,1860172,20170101120000
			 * E441B62395FE07A0A83E6B6725EA62D3,lac$ci,1860172,20170101120000
			 * 17BF5832F7A0CFBC40C8772821E1A97C,lac$ci,1860172,20170101120000
			 * 
			 */
			JavaRDD<String> currData = jsc.textFile(PROPS.getProperty("POS_SNAPSHOT_PATH") + currTime + "/");
			/*
			 * 与fanDis对应关系关联生成区域级别的RDD
			 * key=手机号;
			 * value=区域ID,号段前7位
			 */
			JavaPairRDD<String, String> currDataPair = join2Dis(currData,fanDisDataPair);
			long start = System.currentTimeMillis();
			param.put("hbase_cf", TB_HISTORY_CF_CURRENT);
			param.put("hbase_q", TB_HISTORY_Q_CURRENT);
			doDefalutCalcAndOutput(jsc,param,currDataPair);//计算当前快照
			
			//15分钟快照计算
			if(currTime_15min_ago != null && !currTime_15min_ago.equals("")){
				log.info("======currTime_15min_ago="+currTime_15min_ago);
				JavaRDD<String> historyData_15min_ago = jsc.textFile(PROPS.getProperty("POS_SNAPSHOT_PATH") + currTime_15min_ago + "/");
				JavaPairRDD<String, String> historyDataPair_15min_ago = join2Dis(historyData_15min_ago,fanDisDataPair);
				JavaPairRDD<String, String> people_stop_15min = findStopPeople(currDataPair,historyDataPair_15min_ago);
				param.put("hbase_cf", TB_HISTORY_CF_15_MIN_AGO);
				param.put("hbase_q", TB_HISTORY_Q_STOP);
				doDefalutCalcAndOutput(jsc,param,people_stop_15min);
			}
			//60分钟快照计算
			if(currTime_60min_ago != null && !currTime_60min_ago.equals("") && currTime_60min_ago.endsWith("0000")){
			log.info("======currTime_60min_ago="+currTime_60min_ago);
				JavaRDD<String> historyData_60min_ago = jsc.textFile(PROPS.getProperty("POS_SNAPSHOT_PATH") + currTime_60min_ago + "/");
				JavaPairRDD<String, String> historyDataPair_60min_ago = join2Dis(historyData_60min_ago,fanDisDataPair);
				JavaPairRDD<String, String> people_stop_60min = findStopPeople(currDataPair,historyDataPair_60min_ago);
				param.put("hbase_cf", TB_HISTORY_CF_60_MIN_AGO);
				param.put("hbase_q", TB_HISTORY_Q_STOP);
				doDefalutCalcAndOutput(jsc,param,people_stop_60min);
			}
			
			log.info("======doDefalutCalcAndOutput[defualt] used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
		}
		catch(Exception e){
			log.error("MovementProc error",e);
		}
		finally{
			doRelease(jsc);//释放资源
		}
	}
	
	/**
	 * 发现驻留人群
	 * @param currentData
	 * @param historyData
	 * @return
	 */
	private JavaPairRDD<String, String> findStopPeople(JavaPairRDD<String, String> currentData,JavaPairRDD<String, String> historyData){
		JavaPairRDD<String, Tuple2<String, String>> joinDataPair1 = currentData.join(historyData);
		JavaPairRDD<String, String> rs = joinDataPair1.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
					@Override
					public Iterator<Tuple2<String, String>> call(Tuple2<String, Tuple2<String, String>> arg0)
							throws Exception {
						String key = arg0._1();//手机号密文
						String tabLeft = arg0._2()._1();//区域ID,号段前7位
						String tabRigth = arg0._2()._2();//区域ID,号段前7位
						List<Tuple2<String, String>> returnVal = new ArrayList<Tuple2<String, String>>();
						if(tabLeft != null && !tabLeft.equals("") && tabLeft.equals(tabRigth)){
							//左右表区域完全相同时，判定为驻留人群
							returnVal.add(new Tuple2<String, String>(key,tabLeft));
						}
						return returnVal.iterator();
					}
				}
		);
		return rs;
	}

	/**
	 * 发现进出的移动人群
	 * @param
	 * @param
	 * @return
	 */
	private JavaPairRDD<String, String> findMovedPeople(JavaPairRDD<String, String> leftData,JavaPairRDD<String, String> rightData){
		JavaPairRDD<String, Tuple2<String, org.apache.spark.api.java.Optional<String>>> joinDataPair1 = leftData.leftOuterJoin(rightData);
		JavaPairRDD<String, String> rs = joinDataPair1.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Tuple2<String, org.apache.spark.api.java.Optional<String>>>, String, String>() {
					@Override
					public Iterator<Tuple2<String, String>> call(Tuple2<String, Tuple2<String, org.apache.spark.api.java.Optional<String>>> arg0)
							throws Exception {
						String key = arg0._1();//手机号密文
						String tabLeft = arg0._2()._1();// 区域ID,号段前7位
						String tabRigth = arg0._2()._2().isPresent() ? arg0._2()._2().get() : null;// 区域ID,号段前7位
						List<Tuple2<String, String>> returnVal = new ArrayList<Tuple2<String, String>>();
						if(tabRigth == null){
							//右表无数据，判断该用户从左表区域离开
							returnVal.add(new Tuple2<String, String>(key,tabLeft));
						}else{
							//右表有数据，且与左表对应区域不相等，判定用户出现在左表区域
							if(tabLeft != null && !tabLeft.equals("") && !tabLeft.equals(tabRigth)){
								returnVal.add(new Tuple2<String, String>(key,tabLeft));
							}
						}
						return returnVal.iterator();
					}
				}
		);
		return rs;
	}

	
	/**
	 * 计算历史时间
	 * @param current
	 * @param dateformat
	 * @param duration
	 * @return
	 */
	private String getTimeAgo(String current,String dateformat,int duration){
		try{
			Calendar ca = Calendar.getInstance();
			ca.setTime(DateUtil.strToDate(current, dateformat));
			ca.add(Calendar.MINUTE, duration * -1);
			return DateUtil.dateToStr(ca.getTime(), dateformat);
		}catch(Exception e){
			return null;
		}
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
	protected void doDefalutCalcAndOutput(JavaSparkContext jsc,Map<String,String> param,JavaPairRDD<String, String> currData) throws Exception{
		if(currData == null || currData.isEmpty()){
			log.error("======currData is empty");
			return;
		}
		/*
		 * rdd转化
		 * 转化前：
		 * key=手机号;
		 * value=区域ID,号段前7位
		 * 
		 * 转化后：
		 * key=号段前7位;
		 * value=区域ID
		 */
		JavaPairRDD<String, String> currDataPair1 = currData.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
				String[] arr = arg0._2().split(EntityUtil.DB_RESULT_SPLITSTR);
				if(arr[0] == null || arr[0].equals("")){
					log.error("======currData.mapToPair;arg0="+arg0);
				}
				return new Tuple2<String, String>(arr[1], arr[0]);
			}
		});
		
		JavaRDD<String> uattrData = jsc.textFile(PROPS.getProperty("UFROM_HDFS_PATH"));// 从hdfs读取号段归属地信息
		/*
		 * 统计号段归属地以及总人数。输出格式如下：
		 * key=基站扇区,标签名,标签值		value=人数
		 * 样例：
		 * key=disid,uv,uv			value=100
		 * key=disid,ufrom,310000	value=50
		 * key=disid,ufrom,320000	value=50
		 */
		JavaPairRDD<String, Integer> rsPeopleOrg = calcPeopleOrg(currDataPair1, uattrData);
		
		/*
		 * rdd转化
		 * 转化前：
		 * key=手机号;
		 * value=区域ID,号段前7位
		 * 
		 * 转化后：
		 * key=手机号;
		 * value=区域ID
		 */
		JavaPairRDD<String, String> currDataPair2 = currData.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
				String[] arr = arg0._2().split(EntityUtil.DB_RESULT_SPLITSTR);
				return new Tuple2<String, String>(arg0._1(), arr[0]);
			}
		});
		/*
		 * 根据标签文件数量，依次统计结果。输出格式如下：
		 * key=基站扇区,标签名,标签值		value=人数
		 * 样例：
		 * key=disid,age,25岁以下	value=100
		 * key=disid,age,26-29岁	value=50
		 * key=disid,gender,男		value=50
		 * key=disid,gender,女		value=50
		 * ...
		 */
		log.info("根据标签文件数量，依次关联客户标签并统计结果");
		List<JavaPairRDD<String, Integer>> rsLabelList = calcLabel(jsc, currDataPair2, PROPS);
		
		//求和后,合并结果集
		JavaPairRDD<String, Integer> resultFan = reduceResult(rsPeopleOrg);
		for(JavaPairRDD<String, Integer> rsLabel : rsLabelList){
			resultFan = resultFan.union(reduceResult(rsLabel));
		}
		
		//输出结果
		doOutPut(param, resultFan, null, null);
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
		long start = System.currentTimeMillis();
		log.info("输出结果到hbase");
		writeToHBase(resultFan, param);
		log.info("======writeToHBase(resultSub) used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
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
		final String hbase_cf = param.get("hbase_cf");
		final String hbase_q = param.get("hbase_q");
		
		//考虑到各节点kerbrose认证票据问题引起连接hbase失败，汇聚到driver节点连接
		List<Tuple2<String, String>>  rs = prepareRDDForHBase(result).collect();
		log.info("rs.size()======"+rs.size());

		if(rs == null || rs.isEmpty()){
			return false;
		}
		doWriteHBase(rs, currTime, TB_HISTORY, hbase_cf, hbase_q);
		return true;
	}
	
	
	/**
	 * 转化基站和区域位置rdd
	 * @param jsc
	 * @param fanDisList
	 * @return
	 */
	private JavaPairRDD<String, String> getFanDisDataPair(JavaSparkContext jsc,List<String> fanDisList){
		JavaRDD<String> fanDisData = jsc.parallelize(fanDisList).cache();
		//key=号段，value=行政区
		JavaPairRDD<String, String> fanDisDataPair = fanDisData.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] arr = arg0.split(EntityUtil.DB_RESULT_SPLITSTR);
				return new Tuple2<String, String>(arr[0], arr[1]);
			}
		});
		return fanDisDataPair;
	}
	
	/**
	 * 通过基站id关联上区域id
	 * @param currData
	 * @param fanDisDataPair
	 * @return
	 */
	private JavaPairRDD<String, String> join2Dis(JavaRDD<String> currData, JavaPairRDD<String, String> fanDisDataPair){
		log.info("将currData转化成JavaPairRDD;key=lac$ci;value=手机号密文,号段前7位");
		JavaPairRDD<String, String> currDataPair = currData.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] arr = arg0.split(EntityUtil.DB_RESULT_SPLITSTR);
				return new Tuple2<String, String>(arr[1], arr[0] + EntityUtil.DB_RESULT_SPLITSTR + arr[2]);
			}
		});
		
		JavaPairRDD<String, Tuple2<String, String>> joinDataPair1 = currDataPair.join(fanDisDataPair);
		JavaPairRDD<String, String> rs = joinDataPair1.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
					@Override
					public Iterator<Tuple2<String, String>> call(Tuple2<String, Tuple2<String, String>> arg0)
							throws Exception {
//						String key = arg0._1();
						String tabLeft = arg0._2()._1();//手机号密文,号段前7位
						String tabRigth = arg0._2()._2();//区域
						String[] leftArr = tabLeft.split(EntityUtil.DB_RESULT_SPLITSTR);
						List<Tuple2<String, String>> returnVal = new ArrayList<Tuple2<String, String>>();
						returnVal.add(new Tuple2<String, String>(leftArr[0], tabRigth + EntityUtil.DB_RESULT_SPLITSTR + leftArr[1]));
						return returnVal.iterator();
					}
				}
		);
		
		return rs;
	}
}
