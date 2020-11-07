package com.tanknavy.spark_www.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

import com.alibaba.fastjson.JSONObject;
import com.tanknavy.spark_www.conf.ConfigurationManager;
import com.tanknavy.spark_www.constant.Constants;
import com.tanknavy.spark_www.test.MockData;

public class SparkUtils {
	
	public static void setMaster(SparkConf conf){
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			conf.setMaster("local[2]"); //如果不是local, 放到spark-submit中yarn设定
		}
	}
	
	//public static void mockData(JavaSparkContext sc, SparkSession sparkSession){
	public static void mockData(JavaSparkContext sc, SQLContext sqlContext){
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			MockData.mock(sc, sqlContext);;
		}
	}
	
	public static SQLContext getSQLContext(SparkContext sc) {
	//private static SparkSession getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local){
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc); //HiveContext是SQLContext的子类
		}
	}
	

	/**
	 * 指定时间范围的用户访问行为
	 * @param sqlContext
	 * @param taskParam
	 * @return
	 */
	public static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
		
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE); //避免硬编码，
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		// 从sql上下文中的临时表中读取，临时表就是从hive或者本地读取数据然后注册为spark临时表，这里对应mock_data中的user_visit_action
		String sql = "select * from global_temp.user_visit_action where date>='" + startDate + "' and date <='" + endDate + "'" ; //注意格式
		
		//DataFrame actionDF = sqlContext.sql(sql);
		Dataset<Row> actionDF = sqlContext.sql(sql);
		return actionDF.toJavaRDD(); // DataFrame -> RDD
		//DataFrame->RDD，重新分区，spark sql默认给第一个stage设定了n个task，但是复杂的可能需要1000个
		//return actionDF.toJavaRDD().repartition(10); 
	}
	
	
}
