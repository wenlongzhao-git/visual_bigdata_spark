package com.bigdata.jdbc;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 数据库连接池
 * @author mrq
 *
 */
public class DbPoolConnection implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static DbPoolConnection databasePool = null;
	private static DruidDataSource dds = null;
	static {
		Properties props = new Properties();
		try {
			props.load(DbPoolConnection.class.getClassLoader().getResourceAsStream("dbsource.properties"));
			dds = (DruidDataSource) DruidDataSourceFactory.createDataSource(props);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private DbPoolConnection() {
	}

	public static synchronized DbPoolConnection getInstance() {
		if (null == databasePool) {
			databasePool = new DbPoolConnection();
		}
		return databasePool;
	}

	public DruidPooledConnection getConnection() throws SQLException {
		return dds.getConnection();
	}
	

	/**
	 * 关闭数据库连接
	 * @param conn
	 */
	public static void closeConnection(Connection conn){
		try {
			if(conn != null){
				conn.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}