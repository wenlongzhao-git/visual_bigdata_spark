package com.bigdata.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 数据库连接管理
 * @author mrq
 *
 */
public class DBUtil {

	private static Properties props = new Properties();
	static {
		try {
			props.load(DBUtil.class.getClassLoader().getResourceAsStream("dbsource.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Connection conn = null;
        try {    
            //初始化驱动包     
            Class.forName("oracle.jdbc.driver.OracleDriver");    
            //根据数据库连接字符，名称，密码给conn赋值     
            conn = DriverManager.getConnection(args[0], args[1], args[1]);    
            conn.close();
        } catch (Exception e) {    
            e.printStackTrace();
        }    
	}
	
	/**
	 * 获取数据库连接
	 * @return
	 */
	public static Connection getConnection(){
		Connection conn = null;
        try {    
            //初始化驱动包     
            Class.forName("oracle.jdbc.driver.OracleDriver");    
            //根据数据库连接字符，名称，密码给conn赋值     
            conn = DriverManager.getConnection(props.getProperty("url"), props.getProperty("username"), props.getProperty("password"));    
            return conn;
        } catch (Exception e) {    
            e.printStackTrace();
            return null;
        }    
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
