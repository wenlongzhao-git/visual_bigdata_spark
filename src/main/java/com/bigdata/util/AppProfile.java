package com.bigdata.util;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;


/**
 * 全局app配置类
 * @author mrq
 *
 */
public class AppProfile {
	private Properties props = new Properties();
	private static Logger log = Logger.getLogger(AppProfile.class);
	private static AppProfile ap = new AppProfile();

	private AppProfile(){
		try {
//			props.load(new InputStreamReader(AppProfile.class.getClassLoader().getResourceAsStream("app-dev.properties"), "UTF-8"));
			props.load(new InputStreamReader(AppProfile.class.getClassLoader().getResourceAsStream("app-prod.properties"), "UTF-8"));
			log.info("AppProfile加载完成");
		} catch (IOException e) {
			e.printStackTrace();
			log.error(e);
		}
	}
	
	public static AppProfile getInstance(){
		return ap;
	}

	public Properties getProps() {
		return props;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
