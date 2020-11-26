package com.bigdata.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

	/**
	* 日期转换成字符串
	* @param date 
	* @return str
	*/
	public static String dateToStr(Date date, String formatStr) {
	  
	   SimpleDateFormat format = new SimpleDateFormat(formatStr);
	   String str = format.format(date);
	   return str;
	} 

	/**
	* 字符串转换成日期
	* @param str
	* @return date
	*/
	public static Date strToDate(String dateStr,String formatStr) {
	  
	   SimpleDateFormat format = new SimpleDateFormat(formatStr);
	   Date date = null;
	   try {
	    date = format.parse(dateStr);
	   } catch (ParseException e) {
	    e.printStackTrace();
	   }
	   return date;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//"yyyy-MM-dd HH:mm:ss"
		System.out.println(DateUtil.strToDate("20170101000000", "yyyyMMddHHmmss"));
	}

}
