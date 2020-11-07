package com.tanknavy.spark_www.dataset;

import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;


// With the help of static import, we can access the static members of a class directly without class name or any object
import static org.apache.spark.sql.functions.*; //要使用spark sql中function必须

/**
 * 如何在Java中使用dataset
 * https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html
 * @author admin
 *
 */
public class UserActiveDegreeAnalyze {
	
//In java we need to use encode the bean if we need to use it as schema for the dataset
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession
				.builder()
				.appName("UserActiveDegreeAnalyze")
				.master("local[2]")
				.config("spark.sql.warehouse.dir","E:/input/spark/warehouse")
				.getOrCreate();
		
		Dataset<Row> userBaseInfo = spark.read().json("E:/input/spark/sql/user_base_info2.json");
		Dataset<Row> userActionLog = spark.read().json("E:/input/spark/sql/user_action_log.json");
		//Dataset<Row> userActionLog = spark.read().option("multiline",true).json("E:/input/spark/sql/user_action_log.json");
	    String startDate = "2016-08-01";
	    String endDate = "2019-12-25";
	    userActionLog.printSchema();
	    
	    // 购买金额最多的用户
	    userActionLog
	     .filter("actionTime >=' " + startDate + "' and actionTime <= '" + endDate + "'")
	     .join(userBaseInfo, userActionLog.col("userId").equalTo(userBaseInfo.col("userId"))) // equalTo代替scala ===
	     .groupBy(userBaseInfo.col("userId"), userBaseInfo.col("username"))
	     .agg(round(sum(userActionLog.col("purchaseMoney")),2).alias("totalPurchaseMoney"))
	     .sort(desc("totalPurchaseMoney"))
	     .limit(10)
	     .show(); //测试成功
		
	    // spark sql
	    System.out.println("spark sql------------------");
	    userActionLog.createOrReplaceTempView("action"); // _corrupt_record
	    //userActionLog.createOrReplaceGlobalTempView("action"); //global_temp.action, _corrupt_record
	    //userActionLog.as(Encoders.bean(UserAction.class)).createOrReplaceTempView("action"); //_corrupt_record
	    //userBaseInfo.createOrReplaceTempView("info");
	    spark.sql("select * from action limit 5").show();
	    userActionLog.printSchema();
	    
	    /*
	     * https://www.codota.com/code/java/methods/org.apache.spark.sql.Dataset/map
	     * map中如何encoder
	     */
	    Encoder<UserAction> encoderUser = Encoders.bean(UserAction.class);
	    Encoder<UserActionLogVO> encoderUserVO = Encoders.bean(UserActionLogVO.class);
	    
	    
	    // 一个周期内，购买金额增长最快的用户
	    // DataFrame 使用mapFunction到Dataset<myClass>成功
	    // DataFrame调用.as(Encoders.bean(UserAction.class),在mapFunction未测试
	    Dataset<UserActionLogVO> userPurchaseInFirstPeriod = 
	    //Dataset<Row> userPurchaseInFirstPeriod = //指定类型后就_corrupt_record
	    //userActionLog.as(Encoders.bean(UserAction.class)) //转为DataSet
	    
	    userActionLog // 还是DataFrame
	    		.filter("actionTime >= '2016-10-01' and actionTime <= '2016-10-31' and actionType = 0")
	    		//.filter("actionTime >=' " + startDate + "' and actionTime <= '" + endDate + "'")
	    		//;
	    		//.map(MapFunction<> func , Encoder<> encode);
	    		//.map(MapFunction<UserAction,UserActionLogVO> record -> record., encoderUserVO);
	    		
	    		.map(new MapFunction<Row,UserActionLogVO>() { //Experimental map(MapFunction<T,U> func, Encoder<U> encoder)

					private static final long serialVersionUID = -5846547149385389842L;

					@Override
					public UserActionLogVO call(Row row) throws Exception {
						UserActionLogVO user = new UserActionLogVO();
						//user.setLogId(row.getLong(0)); //按照序号,不工作,因为corrupt_record
						//user.setUserId(row.getLong(1));
						//user.setActionCount(1.0);
// https://www.programcreek.com/java-api-examples/?code=jgperrin/net.jgp.labs.spark/net.jgp.labs.spark-master/src/main/java/net/jgp/labs/spark/l250_map/l020_dataset_book/CsvToDatasetBook.java
						user.setLogId(row.getAs("logId")); //按照名字，因为corrupt_record
						user.setUserId(row.getAs("userId")); //按照名字，因为corrupt_record
						user.setActionCount(1.0); //按照名字,使用java bean,
						
						return user;
						//return new UserActionLogVO(row.getLong(0),row.getLong(1),1.0);
					}
				}, encoderUserVO)
				;
	    		
	    		//.limit(5).show();
	    
	    Dataset<UserActionLogVO> userPurchaseInSecondPeriod = 
	    userActionLog // 还是DataFrame
		.filter("actionTime >= '2016-01-01' and actionTime <= '2016-09-30' and actionType = 0")
		.map(new MapFunction<Row,UserActionLogVO>() { //Experimental map(MapFunction<T,U> func, Encoder<U> encoder)

			private static final long serialVersionUID = -5846547149385389842L;

			@Override
			public UserActionLogVO call(Row row) throws Exception {
				UserActionLogVO user = new UserActionLogVO();
				//user.setLogId(row.getLong(0)); //按照序号,不工作
				//user.setUserId(row.getLong(1));
				//user.setActionCount(1.0);
				// https://www.programcreek.com/java-api-examples/?code=jgperrin/net.jgp.labs.spark/net.jgp.labs.spark-master/src/main/java/net/jgp/labs/spark/l250_map/l020_dataset_book/CsvToDatasetBook.java
				user.setLogId(row.getAs("logId")); //按照名字
				user.setUserId(row.getAs("userId")); //按照名字
				user.setActionCount(-1.0); //按照名字
				
				return user;
				//return new UserActionLogVO(row.getLong(0),row.getLong(1),1.0);
			}
		}, encoderUserVO)
		;
		//.limit(5).show();
	    
	    
	    Dataset<UserActionLogVO> userActionLogDS = userPurchaseInFirstPeriod.union(userPurchaseInSecondPeriod);
	    
	    
	    System.out.println("---见证奇迹的时刻A----");
	    userActionLogDS
	     .join(userBaseInfo, userActionLogDS.col("userId").equalTo(userBaseInfo.col("userId")))
	     .groupBy(userBaseInfo.col("userId"), userBaseInfo.col("username"))
	     .agg(sum(userActionLogDS.col("actionCount")).alias("actionIncr"))
	     .sort(desc("actionIncr"))
	     .limit(10)
	     .show(); //测试成功
	    System.out.println("---见证奇迹的时刻A----");
	    
	    
	    //https://spark.apache.org/docs/2.4.0/sql-getting-started.html
	    //DataFrame调用.as(Encoders.bean(UserAction.class),在mapFunction未测试
	    // dataset -> dataset
	    Dataset<UserAction> userPurchaseInFirstPeriod2 = 
	    //Dataset<UserActionLogVO> userPurchaseInFirstPeriod2 = 
	    userActionLog.as(Encoders.bean(UserAction.class)); //转为DataSet,_corrupt_record
	    //测试dataframe->dataset
	    userPurchaseInFirstPeriod2.printSchema(); //栏位还是有的
	    userPurchaseInFirstPeriod2.limit(5).show();
	  
	    userPurchaseInFirstPeriod2.createOrReplaceTempView("action2"); //dataset到table
	    spark.sql("select * from action2").show(); //只有第一个_corrupt_record有值，其它为空
	    //spark.sql("select userId,logId,purchaseMoney from action2").show(); //全部为空
	    
	    
	    System.out.println("---TEST测试----");
	    //userActionLog // 还是DataFrame
		/*
	    .filter("actionTime >= '2016-10-01' and actionTime <= '2016-10-31' and actionType = 0")
		//.map(MapFunction<> func , Encoder<> encode);
		.map( (MapFunction<UserAction, UserActionLogVO>) t-> 
		//{ return new UserActionLogVO(t.getLogId(), t.getUserId(), t.getPurchaseMoney());}, encoderUserVO);
		{ 	UserActionLogVO user = new UserActionLogVO();
			user.setUserId(t.getLogId());
			user.setLogId(t.getUserId());
			user.setActionCount(t.getPurchaseMoney());
			return user;
		}, encoderUserVO);
	    
	    userPurchaseInFirstPeriod.limit(10).show(); //测试输出
	    
	    /*
	    Dataset<UserActionLogVO> userPurchaseInSecondPeriod2 = 
	    userActionLog.as(Encoders.bean(UserAction.class)) //转为DataSet
	    //userActionLog // 还是DataFrame
		.filter("actionTime >= '2016-01-01' and actionTime <= '2016-09-30' and actionType = 0")
		//.map(MapFunction<> func , Encoder<> encode);
		.map( (MapFunction<UserAction, UserActionLogVO>) t-> 
		//{ return new UserActionLogVO(t.getLogId(), t.getUserId(), t.getPurchaseMoney());}, encoderUserVO);
		{ 	UserActionLogVO user = new UserActionLogVO();
		user.setUserId(t.getLogId());
		user.setLogId(t.getUserId());
		user.setActionCount(t.getPurchaseMoney());
		return user;
		}, encoderUserVO);
	    
	    Dataset<UserActionLogVO> userActionLogDS2 = userPurchaseInFirstPeriod2.union(userPurchaseInSecondPeriod2);
	    
	    
	    System.out.println("---见证奇迹的时刻B----");
	    userActionLogDS2
	     .join(userBaseInfo, userActionLogDS2.col("userId").equalTo(userBaseInfo.col("userId")))
	     .groupBy(userBaseInfo.col("userId"), userBaseInfo.col("username"))
	     .agg(sum(userActionLogDS2.col("actionCount")).alias("actionIncr"))
	     .sort(desc("actionIncr"))
	     .limit(10)
	     .show(); //测试失败，
	    System.out.println("---见证奇迹的时刻B----");
	    
	   */
	    
	    
	}
	
	/*
	// java bean, spark 中要使用dataFrame转换dataset：  DataFrame.as(Encoders.bean(object.class));
	// map(MapFunction<T,U> func, Encoder<U> encoder)
	//In java we need to use encode the bean if we need to use it as schema for the dataset
	// https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/Dataset.html
	//https://databricks.com/blog/2016/01/04/introducing-apache-spark-datasets.html
	
	// 以下scala三行，java bean要写很多
	//为什么要使用case class?,case class里面的栏位名一定要和json文件中match
	case class UserActionLog(logId:Long, userId:Long, actionTime:String, actionType:Long, purchaseMoney:Double)
  
	// 以下两个case用于dataset在map时中间数据类型(dataFrame先要as转为dataset强类型才可能)
	// map中间结果可以不用class类型，直接dataFrame，但是这时候ROW没有栏位名了，只能是_1,_2_3...
	  case class UserActionLogVO(logId:Long,userId:Long,actionCount:Long) // 计算过程中间对象,统计次数
	  case class UserActionLogVM(logId:Long,userId:Long,actionPurchase:Double) // 计算过程中间对象,统计金额
	*/
	static class UserAction implements Serializable{

		private static final long serialVersionUID = 8983784362223767407L;
		private Long logId;
		private Long userId;
		private String actionTime;
		private Long actionType;
		private Double purchaseMoney;
		
		public Long getLogId() {
			return logId;
		}
		public void setLogId(Long logId) {
			this.logId = logId;
		}
		public Long getUserId() {
			return userId;
		}
		public void setUserId(Long userId) {
			this.userId = userId;
		}
		public String getActionTime() {
			return actionTime;
		}
		public void setActionTime(String actionTime) {
			this.actionTime = actionTime;
		}
		public Long getActionType() {
			return actionType;
		}
		public void setActionType(Long actionType) {
			this.actionType = actionType;
		}
		public Double getPurchaseMoney() {
			return purchaseMoney;
		}
		public void setPurchaseMoney(Double purchaseMoney) {
			this.purchaseMoney = purchaseMoney;
		}
		
	}
	
	
	public static class UserActionLogVO implements Serializable{
		
		private static final long serialVersionUID = -1417359984213800356L;
		private Long logId;
		private Long userId;
		private Double actionCount;
		
		public UserActionLogVO() {
		//public UserActionLogVO(Long logId,Long userId,Double actionCount) {
			//super();
			this.logId = logId;
			this.userId = userId;
			this.actionCount = actionCount;
		}
		
		/*
		public UserActionLogVO(Long logId,Long userId,Double actionCount) {
			//super();
			this.logId = logId;
			this.userId = userId;
			this.actionCount = actionCount;
		}*/
		
		public Long getLogId() {
			return logId;
		}
		public void setLogId(Long logId) {
			this.logId = logId;
		}
		public Long getUserId() {
			return userId;
		}
		public void setUserId(Long userId) {
			this.userId = userId;
		}
		public Double getActionCount() {
			return actionCount;
		}
		public void setActionCount(Double actionCount) {
			this.actionCount = actionCount;
		}
		
		
		
	}

}


class test{
	
}