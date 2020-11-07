package com.tanknavy.spark_www.product;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import com.alibaba.fastjson.JSONObject;
import com.tanknavy.spark_www.conf.ConfigurationManager;
import com.tanknavy.spark_www.constant.Constants;
import com.tanknavy.spark_www.dao.ITaskDAO;
import com.tanknavy.spark_www.dao.impl.DAOFactory;
import com.tanknavy.spark_www.domain.Task;
import com.tanknavy.spark_www.util.ParamUtils;
import com.tanknavy.spark_www.util.SparkUtils;

/**
 * 
 * @author admin
 *
 */
public class AreaTop3ProductSpark {
	
	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("AreaTop3ProductSpark");
		SparkUtils.setMaster(conf);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc()); //本地模式就是SQLContext,production则是HiveContext从hive查询数据
		SparkSession spark=SparkSession.builder().appName("Spark_read_mySQL").getOrCreate(); // 同上，新版
		
		SparkUtils.mockData(sc, sqlContext); // 本地模式产生数据和注册临时表
		
		// 查询任务参数
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		long taskid = ParamUtils.getTaskIdFromArgsLocal(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
		Task task = taskDAO.findById(taskid); //按照任务id从mySQL读取task任务参数
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam()); // 解析任务参数的json字符串
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate =   ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		//System.out.println(startDate);
		//System.out.println(endDate);

		//获取用户指定时间范围过滤后的点击数据
		JavaPairRDD<Long, Row> clickActionDF = getClickActionRDDbyDate(sqlContext,startDate,endDate);
		//System.out.println("TEST-----------------------");
		System.out.println(clickActionDF.take(1));
		
		//从mysql中查询城市信息
		JavaPairRDD<Long, Row> cityInfoRDD = getCityInfoRDD(sqlContext); //旧版
		//JavaRDD<Row> cityInfoRDD2 = getCityInfoRDD2(spark); //新版
		System.out.println(cityInfoRDD.take(1)); // [0,北京,华北]
		
		//点击rdd和城市rdd的join,必须是JavaPairRDD k/v类型
		JavaPairRDD<Long, Tuple2<Row, Row>> ActionCityJoinRDD = clickActionDF.join(cityInfoRDD);
		System.out.println(ActionCityJoinRDD.take(5));
		
		
		
		//关闭spark context
		sc.close();
		
	}
	
	
	
	/** clickProduct不为空，并且时间在用户选定范围之类
	 * @param sqlContext
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	private static JavaPairRDD<Long, Row> getClickActionRDDbyDate(SQLContext sqlContext,String startDate,String endDate){
		String sql_test = "select city_id, click_product_id as product_id from global_temp.user_visit_action "
				+ "where date>='" + startDate + "' and date <='" + endDate + "'"
				+ " AND click_product_id IS NOT NULL"
				//+ " AND click_product_id != 'NULL'" //加上就报错
			    //+ " AND click_product_id != 'null'"
				; //注意格式
		//防止源数据本来是NULL类型，结果导入hive后变成字符串的NULL
		String sql = "SELECT "
						+ "city_id,"
						+ "click_product_id as product_id "
				        + "FROM global_temp.user_visit_action "
						+ "WHERE click_product_id IS NOT NULL "
				        + "AND click_product_id != 'NULL' "
						+ "AND click_product_id != 'null' "
				       // + "AND action_time>='" + startDate + "' " //action_time精确到秒，date是天
						+ "AND date >='" + startDate + "' " 
				       // + "AND action_time<='" + endDate +"'";
					    + "AND date <='" + endDate +"'";
				
		//DataFrame actionDF = sqlContext.sql(sql);
		Dataset<Row> clickActionDF = sqlContext.sql(sql_test);
		//Dataset<Row> clickActionDF = sqlContext.sql(sql);
		//return clickActionDF.javaRDD(); // DataFrame -> RDD
		//return clickActionDF.toJavaRDD().repartition(10); // DataFrame -> RDD
		
		// 返回JavaPairRDD类型
		JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();
		JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(new PairFunction<Row, Long, Row>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<Long, Row> call(Row row) throws Exception {
				return new Tuple2<Long, Row>(row.getLong(0), row);
			}
		});
		
		return cityid2clickActionRDD;
		
	}
	
	
	/**旧版
	 * 使用spark SQL从mySQL中查询城市信息
	 * @param sqlContext
	 * @return
	 */
	private static JavaPairRDD<Long, Row> getCityInfoRDD(SQLContext sqlContext){
		// MySQL连接配置信息
		// https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
		String url = null;
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			url = ConfigurationManager.getProperty(Constants.JDBC_URL);
		} else{
			url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
		}
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", url);
		options.put("dbtable", "city_info");
		options.put("user", ConfigurationManager.getProperty(Constants.JDBC_USER));
		options.put("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD));

		// 旧版：通过SQLContext去从mysql中查询数据
		Dataset<Row> cityInfoDF = sqlContext.read().format("jdbc").options(options).load();
		// 新版spark sql
		//SparkSession spark=SparkSession.builder().appName("Spark_read_mySQL").master("local[2]").enableHiveSupport().getOrCreate();
		//SparkSession spark=SparkSession.builder().appName("Spark_read_mySQL").getOrCreate();
		//Dataset<Row> cityInfoDF2 = spark.read().format("jdbc").options(options).load();
		
		//return cityInfoDF.toJavaRDD();
		
		JavaPairRDD<Long, Row> cityInfo2RDD = cityInfoDF.javaRDD().mapToPair(new PairFunction<Row, Long, Row>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<Long, Row> call(Row row) throws Exception {
				return new Tuple2<Long, Row>(Long.valueOf(row.getInt(0)), row); //从mysql表city_info第一个栏位，是int类型
			}
		});
		return cityInfo2RDD;
		
	}
	
	
	/**新版
	 * 使用spark SQL从mySQL中查询城市信息
	 * @param sqlContext
	 * @return
	 */
	private static JavaRDD<Row> getCityInfoRDD2(SparkSession spark){
		// MySQL连接配置信息
		// https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
		String url = null;
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			url = ConfigurationManager.getProperty(Constants.JDBC_URL);
		} else{
			url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
		}
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", url);
		options.put("dbtable", "city_info");
		options.put("user", ConfigurationManager.getProperty(Constants.JDBC_USER));
		options.put("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD));

		// 旧版：通过SQLContext去从mysql中查询数据
		//Dataset<Row> cityInfoDF = sqlContext.read().format("jdbc").options(options).load();
		// 新版spark sql
		//SparkSession spark=SparkSession.builder().appName("Spark_read_mySQL").master("local[2]").enableHiveSupport().getOrCreate();
		//SparkSession spark=SparkSession.builder().appName("Spark_read_mySQL").getOrCreate();
		Dataset<Row> cityInfoDF2 = spark.read().format("jdbc").options(options).load();

		return cityInfoDF2.toJavaRDD();
		
	}
	
	
}
