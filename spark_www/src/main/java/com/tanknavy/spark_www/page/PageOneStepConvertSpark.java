package com.tanknavy.spark_www.page;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.tanknavy.spark_www.dao.ITaskDAO;
import com.tanknavy.spark_www.dao.impl.DAOFactory;
import com.tanknavy.spark_www.domain.Task;
import com.tanknavy.spark_www.util.ParamUtils;
import com.tanknavy.spark_www.util.SparkUtils;

public class PageOneStepConvertSpark {

	public static void main(String[] args) {

		//1.根据配置设置SparkConf的master和上下文
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc()); // 如果不是Local，就为HiveContext
		SparkUtils.setMaster(conf);
		
		
		//2.生成模拟数据
		SparkUtils.mockData(sc, sqlContext);
		
		
		//3.查询任务
		if(args.length == 0){args = new String[]{"11"};} 	//TEST:本应该用命令行参数读取
		long taskid = ParamUtils.getTaskIdFromArgs(args);
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskid);
		
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam()); // 从表中某个text栏位解析json字符串
		
		//4.查询指定日期过
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		
	}

}
