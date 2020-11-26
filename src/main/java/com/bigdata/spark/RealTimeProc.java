package com.bigdata.spark;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 实时计算类
 * @author mrq
 *
 */
public class RealTimeProc extends SnapshotProc2{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger log = Logger.getLogger(RealTimeProc.class);
	public static final String TB_REALTIME = "beho:tb_shurta_result_realtime";//历史快照计算结果存放表名
	public static final String TB_REALTIME_CF = "current";//列族名，当前人群画像
	public static final String TB_REALTIME_CF_Q1 = "labval";//列名，当前人群画像标签值，json字符串{标签值1:人数1,标签值2:人数2}

	public static void main(String[] args) {
		Map<String,String> param = getParaMap(args);
		SnapshotProc2 rtp = new RealTimeProc();
		String appname = param.get("appName");
		if(appname != null && !appname.equals("")){
			rtp.setAppName(appname);
		}else{
			rtp.setAppName("RealTimeProc");
		}
		long start = System.currentTimeMillis();
		log.info("======procNam:"+param.get("procName"));
		rtp.doDefault(param);
		log.info("======used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
	}
	
	
	/**
	 * 设置参数
	 * @param args
	 * @return
	 */
	public static Map<String,String> getParaMap(String[] args){
		if(args.length > 0){
			Map<String,String> map = new HashMap<String,String>();
			if("default".equals(args[0])){
				if(args.length != 4){
					System.err.println("Usage: RealTimeProc <procName> <crn> <appName> <outPutType>");
					System.exit(1);
				}
				map.put("procName", args[0]);
				map.put("crn", args[1]);
				map.put("appName", args[2]);
				map.put("outPutType", args[3]);//支持输出结果到hdfs、hbase 或者 both
			}
			return map;
		}
		return null;
	}

	/**
	 * 持久化快照
	 * @param locData
	 * @param param
	 * @param savePath
	 */
	protected void writeSnapshot2hdfs(JavaRDD<String> locData,Map<String,String> param, String savePath){
		//实时计算不需要持久化，只是为重写父类方法
		return;
	}
	
	/**
	 * 输出结果
	 */
	/*protected void doOutPut(Map<String,String> param,JavaPairRDD<String, Integer> resultFan,
			JavaPairRDD<String, Integer> resultGrid,JavaPairRDD<String, Integer> resultSub){
		//计算结束输出结果
		long start = System.currentTimeMillis();
		log.info("输出结果到hbase");
		writeToHBase(resultGrid, param);
		log.info("======writeToHBase(resultGrid) used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
		start = System.currentTimeMillis();
		writeToHBase(resultSub, param);
		log.info("======writeToHBase(resultSub) used [" + (System.currentTimeMillis() - start) / 1000 + "] seconds ..");
	}*/
	
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
		
		//考虑到各节点kerbrose认证票据问题引起连接hbase失败，汇聚到driver节点连接
		List<Tuple2<String, String>>  rs = prepareRDDForHBase(result).collect();
		log.info("rs.size()======"+rs.size());
	
		if(rs == null || rs.isEmpty()){
			return false;
		}
		doWriteHBase(rs, null, TB_REALTIME, TB_REALTIME_CF, TB_REALTIME_CF_Q1);
		return true;
	}
	
}
