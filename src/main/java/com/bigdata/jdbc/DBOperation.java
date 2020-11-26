package com.bigdata.jdbc;

import com.bigdata.util.EntityUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库操作类
 * 
 * @author mrq
 *
 */
public class DBOperation {

	private static Logger log = Logger.getLogger(DBOperation.class);
//	private static DbPoolConnection pool = DbPoolConnection.getInstance();

	public static void main(String[] args) {
	}

	
	/**
	 * 获取配置,基站扇区与grid的关系
	 * @return
	 */
	public static List<String> getFanGrid(){
		Connection conn = null;
		List<String> returnval = new ArrayList<String>();
		String sql = "select t.site_fan,t.gridid,t.sratio from tb_base_site_grid_square t";
		try {
			long start = System.currentTimeMillis();
//			conn = pool.getConnection();
			conn = DBUtil.getConnection();
			log.info("======getFanGrid getConnection used [" + (System.currentTimeMillis() - start) + "] ms");
			PreparedStatement stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				returnval.add(rs.getString("site_fan") + EntityUtil.DB_RESULT_SPLITSTR + rs.getString("gridid") + EntityUtil.DB_RESULT_SPLITSTR + rs.getDouble("sratio"));
			}
			return returnval;
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			return null;
		} finally {
			DbPoolConnection.closeConnection(conn);
		}
	}

	/**
	 * 获取配置,grid与热点区域的关系
	 * @return
	 */
	public static Map<String,List<String>> loadListParam(){
		Map<String,List<String>> returnval = new HashMap<String,List<String>>();
		List<String> gridDisRelations = new ArrayList<String>();
		List<String> fanDisRelations = new ArrayList<String>();
		List<String> disDisRelations = new ArrayList<String>();

//		String sql = "select dg.gridid,dg.disid from tb_lbs_district_grid dg,tb_lbs_district_info di where di.ispoi='1' and di.disid=dg.disid and di.disabled='0' "
//				+ " union all select g.gridid,'CAN_'||g.district disid from tb_base_grid_info g where g.gtype='1' and g.disabled='0'";
		String sql1 = "select gridid,disid from v_grid_dis_list";
		String sql2 = "select fan,disid from v_fan_dis_list";
		String sql3 = "select scode,disid from v_dis_dis_list";

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			long start = System.currentTimeMillis();
//			conn = pool.getConnection();
			conn = DBUtil.getConnection();
			log.info("======loadListParam getConnection used [" + (System.currentTimeMillis() - start) + "] ms");
			stmt = conn.prepareStatement(sql1);
			rs = stmt.executeQuery();
			while(rs.next()){
				gridDisRelations.add(rs.getString("gridid") + EntityUtil.DB_RESULT_SPLITSTR + rs.getString("disid"));
			}
			log.info("======gridDisRelations returnval-size["+gridDisRelations.size()+"] used [" + (System.currentTimeMillis() - start) + "] ms");

			stmt = conn.prepareStatement(sql2);
			rs = stmt.executeQuery();
			while(rs.next()){
				fanDisRelations.add(rs.getString("fan") + EntityUtil.DB_RESULT_SPLITSTR + rs.getString("disid"));
			}
			log.info("======fanDisRelations returnval-size["+fanDisRelations.size()+"] used [" + (System.currentTimeMillis() - start) + "] ms");

			stmt = conn.prepareStatement(sql3);
			rs = stmt.executeQuery();
			while(rs.next()){
				disDisRelations.add(rs.getString("scode") + EntityUtil.DB_RESULT_SPLITSTR + rs.getString("disid"));
			}
			log.info("======disDisRelations returnval-size["+disDisRelations.size()+"] used [" + (System.currentTimeMillis() - start) + "] ms");

			returnval.put("gridDisList", gridDisRelations);
			returnval.put("fanDisList", fanDisRelations);
			returnval.put("disDisList", disDisRelations);

			return returnval;
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			return null;
		} finally {
			DbPoolConnection.closeConnection(conn);
		}
	}

	/**
	 * 获取配置,sitfan与district的关系
	 * @return
	 */
	public static Map<String,List<String>> loadListParamHistory(){
		Map<String,List<String>> returnval = new HashMap<String,List<String>>();
		List<String> fanDisRelations = new ArrayList<String>();
		String sql = "select fan,disid from v_history_fan_dis_list";
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			long start = System.currentTimeMillis();
			conn = DBUtil.getConnection();
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			while(rs.next()){
				fanDisRelations.add(rs.getString("fan") + EntityUtil.DB_RESULT_SPLITSTR + rs.getString("disid"));
			}
			log.info("======fanDisRelations returnval-size["+fanDisRelations.size()+"] used [" + (System.currentTimeMillis() - start) + "] ms");
			returnval.put("fanDisList", fanDisRelations);
			return returnval;
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			return null;
		} finally {
			DbPoolConnection.closeConnection(conn);
		}
	}
}
