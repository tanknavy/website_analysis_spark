package com.tanknavy.spark_www.test;
import java.text.DateFormat; //抽象类
import java.text.SimpleDateFormat; //实现类，线程不安全，局部使用
import java.util.Date;//
import java.util.Calendar;

// from jdk 1.8开始新的日期时间类型
import java.time.*;
import java.time.format.*;

import scala.reflect.internal.Trees.New;

import com.tanknavy.spark_www.util.DateUtils;

@SuppressWarnings("unused")
public class DateTimeTest {

	final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //线程不安全
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Date date = new Date();
		System.out.println(date); // 默认类型格式
		System.out.println(dateFormat.format(date));
		Object object = 123;
		System.out.println(object.toString());
		
		// from java1.8
		
		
		//方法中的内部类，只能是final或者abstract类型
		final class Tclass {
			String field;

			public Tclass(String p){
				this.field = p;
			}
		}
		
		Tclass tclass = new Tclass("inner class in method");
		System.out.println(tclass.field);
		
		System.out.println(new Date());
		System.out.println(new Date().getTime());
		
		
		System.out.println(DateUtils.parseDateKey("2019-12-07"));
		System.out.println(DateUtils.parseDateKey("20191207"));
	}

}
