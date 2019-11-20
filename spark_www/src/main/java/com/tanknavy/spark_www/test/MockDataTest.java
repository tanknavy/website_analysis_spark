package com.tanknavy.spark_www.test;

import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

import com.tanknavy.spark_www.constant.Constants;
import com.tanknavy.spark_www.util.DateUtils;

/* 从2.0开始SparkSession取代SQLContext作为对结构化数据(row & columns)访问的入口
 * SQLContext -> SparkSession
 * https://databricks.com/blog/2016/08/15/how-to-use-sparksession-in-apache-spark-2-0.html
 * https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
 */

public class MockDataTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf()
		.setAppName(Constants.SPARK_APP_NAME_SESSION)
		.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		//SQLContext: The entry point for working with structured data (rows and columns) in Spark 1.x. 
		//As of Spark 2.0, this is replaced by [[SparkSession]]. 
		SQLContext sqlContext = new SQLContext(sc); // 从本地内存读
		//SQLContext sqlContext2 = new HiveContext(sc); // (旧版)从hive读取,
		
		MockData.mock(sc, sqlContext); //测试成功
		//MockData.mock(sc, sqlContext2); //测试hive失败, 由于maven类冲突，原因应该是需要spark-hive更古老的类
		
		
		/*spark 访问hive, 2.0以后
		 * http://belablotski.blogspot.com/2016/01/access-hive-tables-from-spark-using.html
		 */
		//SparkSession spark=SparkSession.builder().appName("Spark_read_Hive").enableHiveSupport().getOrCreate();
		//注意：增加master配置
		SparkSession spark=SparkSession.builder().appName("Spark_read_Hive").master("local[2]").enableHiveSupport().getOrCreate();
		spark.sparkContext().conf().set("spark.sql.warehouse.dir", "/user/hive/warehouse");
		//spark.sparkContext().conf().set("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse");
		//spark.sparkContext().conf().set("hive.metastore.uris", "thrift://localhost:10000");
		//spark.sparkContext().conf().set("hive.metastore.uris", "thrift://localhost:9083"); //metastore端口
		//spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive"); // 失败
		//spark.sql("show tables").show(); //找不到任何表？
		//spark.sql("select * from stocks limit 5").show();
	    //spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");
	    //spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show(); //table join
		
		//MockDataSession.mock(sc, spark); //测试SparkSession成功
		
		
		// action_time时间测试
		//Date actionTime = DateUtils.parseTime(row.getString(4));
		
		String sql = "select * from global_temp.user_visit_action" ; //注意格式
		
		//DataFrame actionDF = sqlContext.sql(sql);
		Dataset<Row> actionDF = sqlContext.sql(sql);
		JavaRDD<Row> actionRDD = actionDF.toJavaRDD();
		System.out.println(actionRDD.count());
		//第二版：计算session时长(开始和结束时间)和步长
		//System.out.println("TEST:actionTime-------------------------------");
		//System.out.println(row.getString(4));
		//Date actionTime = DateUtils.parseTime(row.getString(4));//表user_visit_action对应的action_time字符串栏位
		//System.out.println(actionTime); //
		JavaRDD<Date> timeRDD =actionRDD.map(new Function<Row, Date>() {
			
			@Override
			public Date call(Row row) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(row.getString(4));
				//if(!row.isNullAt(4) && row.getString(4) != ""){
					Date actionTime = DateUtils.parseTime(row.getString(4)); //运行时时间格式转换报错
				
				//Date actionTime = row.getDate(4); //试试spark自带的时间提取格式
				//System.out.println(actionTime);
				return actionTime;
				//}
				//return null;
			}
		
		});
		System.out.println(timeRDD.count());
		System.out.println(timeRDD.take(10).toString());
		
		
		
	}

}
