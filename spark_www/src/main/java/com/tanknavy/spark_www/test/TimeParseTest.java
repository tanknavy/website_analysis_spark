package com.tanknavy.spark_www.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import com.tanknavy.spark_www.util.DateUtils;
import com.tanknavy.spark_www.util.StringUtils;

public class TimeParseTest {
	
	public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//String testTime = "2019-11-18 13:33:03";
		//Date actionTime = DateUtils.parseTime(row.getString(4)); //在RDD中运行时时间格式转换报错
		
		for (int i = 0; i < 1000; i++) {
			Random random = new Random();
			String date = DateUtils.getTodayDate();
			String baseActionTime = date + " " + random.nextInt(23);
			String actionTime = baseActionTime + ":"
					+ StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
					+ ":"
					+ StringUtils.fulfuill(String.valueOf(random.nextInt(59))); // 追加上分钟和秒

			Date getTime = DateUtils.parseTime(actionTime);
			System.out.println(getTime);
		}
	}
	
	public static Date parseTime2(String time){
		try {
			return TIME_FORMAT.parse(time); // SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
