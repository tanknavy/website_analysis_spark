package com.tanknavy.spark_www.util;

import java.text.ParseException;
import java.text.SimpleDateFormat; // formatting (date → text), parsing (text → date), and normalization
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience.Public;

import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion.Static;

/**
 * SimpleDateFormat is a concrete class for formatting and parsing dates in a locale-sensitive manner. 
 * It allows for formatting (date → text), parsing (text → date), and normalization
 * 日期时间工具类
 * @author Administrator
 *
 */
public class DateUtils {
	
	public static final SimpleDateFormat TIME_FORMAT = 
			new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat DATE_FORMAT = 
			new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * 判断一个时间是否在另一个时间之前
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return 判断结果
	 */
	public static boolean before(String time1, String time2) {
		try {
			Date dateTime1 = TIME_FORMAT.parse(time1);
			Date dateTime2 = TIME_FORMAT.parse(time2);
			
			if(dateTime1.before(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 判断一个时间是否在另一个时间之后
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return 判断结果
	 */
	public static boolean after(String time1, String time2) {
		try {
			Date dateTime1 = TIME_FORMAT.parse(time1);
			Date dateTime2 = TIME_FORMAT.parse(time2);
			
			if(dateTime1.after(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 计算时间差值（单位为秒）
	 * @param time1 时间1
	 * @param time2 时间2
	 * @return 差值
	 */
	public static int minus(String time1, String time2) {
		try {
			Date datetime1 = TIME_FORMAT.parse(time1);
			Date datetime2 = TIME_FORMAT.parse(time2);
			
			long millisecond = datetime1.getTime() - datetime2.getTime();
			
			return Integer.valueOf(String.valueOf(millisecond / 1000));  
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取年月日和小时
	 * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
	 * @return 结果
	 */
	public static String getDateHour(String datetime) {
		String date = datetime.split(" ")[0];
		String hourMinuteSecond = datetime.split(" ")[1];
		String hour = hourMinuteSecond.split(":")[0];
		return date + "_" + hour;
	}  
	
	/**
	 * 获取当天日期（yyyy-MM-dd）
	 * @return 当天日期
	 */
	public static String getTodayDate() {
		return DATE_FORMAT.format(new Date());  
	}
	
	/**
	 * 获取昨天的日期（yyyy-MM-dd）
	 * @return 昨天的日期
	 */
	public static String getYesterdayDate() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());  
		cal.add(Calendar.DAY_OF_YEAR, -1);  
		
		Date date = cal.getTime();
		
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化日期（yyyy-MM-dd）
	 * @param date Date对象
	 * @return 格式化后的日期
	 */
	public static String formatDate(Date date) {
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化时间（yyyy-MM-dd HH:mm:ss）
	 * @param date Date对象
	 * @return 格式化后的时间
	 */
	public static String formatTime(Date date) {
		return TIME_FORMAT.format(date);
	}
	
	/*
	 * 解析时间字符串
	 */
	public static Date parseTime(String time){
		/* https://medium.com/@daveford/numberformatexception-multiple-points-when-parsing-date-650baa6829b6
		   SimpleDateFormat不是线程安全的，在这个Spark RDD<Row>中并行时不稳定，改为local variable
		   Date formats are not synchronized.
		 * It is recommended to create separate format instances for each thread.
		 * If multiple threads access a format concurrently, it must be synchronized
		 * externally.
		*/
		final SimpleDateFormat TIME_FORMAT_LOCAL = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //解决在RDD<Row>中解析报错
		try {
			//return TIME_FORMAT.parse(time); // SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			return TIME_FORMAT_LOCAL.parse(time); // SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 日期格式格式化为"yyyyMMdd"格式
	 * @param date
	 * @return
	 */
	public static String formatDateKey(Date date){
		final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");
		return DATEKEY_FORMAT.format(date);
	}
	
	/**
	 * 字符串日期解析为"yyyyMMdd"时间格式
	 * @param date
	 * @return
	 */
	public static Date parseDateKey(String date) {
		final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");
		try {
			return DATEKEY_FORMAT.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 格式化日期到分钟级别"yyyyMMddHHmm"
	 * @param date
	 * @return
	 */
	public static String formatTimeMinute(Date date){
		final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");
		return DATEKEY_FORMAT.format(date);
	}
	
}
