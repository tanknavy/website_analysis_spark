package com.tanknavy.spark_www.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.tanknavy.spark_www.util.DateUtils;
import com.tanknavy.spark_www.util.StringUtils;

public class MockDataSession {

	//public static void mock(JavaSparkContext sc, SQLContext sqlContext){ // 1.6版
	public static void mock(JavaSparkContext sc, SparkSession sparkSession){ // 2.0版使用SparkSession
		List<Row> rows = new ArrayList<Row>();
		
		String[] searchKeyWords = new String[]{"iPhone","iPad","iWatch","Samsung s10","pixel","iPod","SSD","Laptop","vocation","hotel","ticket"};
		String[] actions = new String[]{"search","click","order","pay"};
		String date = DateUtils.getTodayDate();
		Random random = new Random();
		
		for(int i=0;i<100;i++){ //每个用户
			long userid = random.nextInt(100);
			
			for(int j=0;j<10;j++){ // 每个用户的连接和会话
				String sessionid = UUID.randomUUID().toString().replace("-", "");
				String baseActionTime = date + " " + random.nextInt(23); //随机日期 + 随机时间
				
				Long clickCategoryId = null;
				
				for(int k=0;k<random.nextInt(100);k++){ //某个个用户的某个会话的某个行为
					long pageid = random.nextInt(10);
					String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) 
							+ ":"  + StringUtils.fulfuill(String.valueOf(random.nextInt(59))); // 追加上分钟和秒
					String searchKeyword = null;
					Long clickProductId = null;
					String orderCategoryIds = null;
					String orderProductIds = null;
					String payCategoryIds = null;
					String payProductIds = null;
					
					String action = actions[random.nextInt(actions.length -1)];
					if ("search".equals(action)){
						searchKeyword = searchKeyWords[random.nextInt(searchKeyWords.length - 1)];
					} else if ("click".equals(action)){
						if(clickCategoryId == null){
							clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
						}
						clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
					} else if ("order".equals(action)){
						orderCategoryIds  = String.valueOf(random.nextInt(100));
						orderProductIds = String.valueOf(random.nextInt(100));
					} else if ("pay".equals(action)){
						payCategoryIds = String.valueOf(random.nextInt(100));
						payProductIds = String.valueOf(random.nextInt(100));
					}
					
					Row row = RowFactory.create(date,userid, sessionid, 
							pageid, actionTime, searchKeyword,
							clickCategoryId, clickProductId,
							orderCategoryIds, orderProductIds,
							payCategoryIds, payProductIds, 
							Long.valueOf(String.valueOf(random.nextInt(10))));
					
					rows.add(row);
				}
				
			}
			
		}
		
		JavaRDD<Row> rowsRDD = sc.parallelize(rows); //并行处理rdd
		// 对应session_detail表结构
		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("session_id", DataTypes.StringType, true),
				DataTypes.createStructField("page_id", DataTypes.LongType, true),
				DataTypes.createStructField("action_time", DataTypes.StringType, true),
				DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
				DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
				DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
				DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("city_id", DataTypes.LongType, true)
				));
		//Dataset<Row> rfDataset = spark.createDataFrame(rowRDD, rfSchema); //从rdd创建dataFrame
		//DataFrame df = sqlContext.createDataFrame(rowsRDD, schema); // DataFrame是DataSet的一种特殊类型
		//Dataset<Row> df = sqlContext.createDataFrame(rowsRDD, schema); // 从sqlContext中创建dataframe
		Dataset<Row> df = sparkSession.createDataFrame(rowsRDD, schema); // SparkSession
		//df.registerTempTable("user_visit_action"); // 过时了
		//df.createGlobalTempView("user_visit_action"); //
		df.createOrReplaceGlobalTempView("user_visit_action"); //可能创建不成功
		
		//for(Row _row: df.take(1)){
		for(Row _row: df.takeAsList(1)){ //取第一行数据测试
			System.out.println(_row);
		}
		
		
		rows.clear();
		String[] sexes = new String[]{"male", "female"};
		for(int i = 0; i < 100; i ++) {
			long userid = i;
			String username = "user" + i;
			String name = "name" + i;
			int age = random.nextInt(60);
			String professional = "professional" + random.nextInt(100);
			String city = "city" + random.nextInt(100);
			String sex = sexes[random.nextInt(2)];
			
			Row row = RowFactory.create(userid, username, name, age, 
					professional, city, sex);
			rows.add(row);
		}
		
		rowsRDD = sc.parallelize(rows);
		
		StructType schema2 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("username", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true),
				DataTypes.createStructField("professional", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true)));
		
		//DataFrame df2 = sqlContext.createDataFrame(rowsRDD, schema2);
		Dataset<Row> df2 = sparkSession.createDataFrame(rowsRDD, schema2);
		for(Row _row : df2.takeAsList(1)) {
			System.out.println(_row);  
		}
		
		//df2.registerTempTable("user_info");
		//df.createGlobalTempView("user_visit_action"); //
		df.createOrReplaceGlobalTempView("user_info");
		
		/**
		 * ==================================================================
		 */
		rows.clear();
		
		int[] productStatus = new int[]{0, 1};
		
		for(int i = 0; i < 100; i ++) {
			long productId = i;
			String productName = "product" + i;
			String extendInfo = "{\"product_status\": " + productStatus[random.nextInt(2)] + "}";    
			
			Row row = RowFactory.create(productId, productName, extendInfo);
			rows.add(row);
		}
		
		rowsRDD = sc.parallelize(rows);
		
		StructType schema3 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("product_id", DataTypes.LongType, true),
				DataTypes.createStructField("product_name", DataTypes.StringType, true),
				DataTypes.createStructField("extend_info", DataTypes.StringType, true)));
		
		//DataFrame df3 = sqlContext.createDataFrame(rowsRDD, schema3);
		Dataset<Row> df3 = sparkSession.createDataFrame(rowsRDD, schema3);
		for(Row _row : df3.takeAsList(1)) {
			System.out.println(_row);  
		}
		
		//df3.registerTempTable("product_info");
		//df.createGlobalTempView("user_visit_action"); //
		df.createOrReplaceGlobalTempView("product_info");
		
		
	}

}

