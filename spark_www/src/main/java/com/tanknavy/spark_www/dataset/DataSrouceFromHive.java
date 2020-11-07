package com.tanknavy.spark_www.dataset;

import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;


/**
 * https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
 * @author admin
 *https://blog.csdn.net/cl2010abc/article/details/84777070
 */
public class DataSrouceFromHive {
	
	public static void main(String[] args) {
		
		//spark.sparkContext().conf().set("spark.sql.warehouse.dir", "/user/hive/warehouse");
		//spark.sparkContext().conf().set("hive.metastore.uris", "thrift://localhost:10000");
		String hiveWarehouse = "hdfs://master:9000/user/hive/warehouse";
		String fileWarehouse = "E:/input/spark/warehouse";
		
		SparkSession spark = SparkSession
				.builder()
				.appName("UserActiveDegreeAnalyze")
				.master("local[2]") //本地模式
				//spark standalone模式：在spark_env.sh和slaves中配置spark集群，spar/sbin/start-all.sh启动spark集群
				//spark_defaults配置文件中查看
				//.master("spark://localhost:8080") 
				// 要使用yanr模式在spark_env.sh配置中Ensure that HADOOP_CONF_DIR or YARN_CONF_DIR points to the directory
				//.master("yarn") // --deploy-mode cluster
				.config("spark.sql.warehouse.dir",hiveWarehouse) // 在spark_defaults配置文件中查看
				//.config("hive.metastore.uris", "thrift://localhost:10000") // metastore服务
				//.config("hive.metastore.uris", "jdbc:hive2://localhost:10000") //
				// hive --service metastore
				// hive --service hiveserver2
				// Beeline:	 
				// !connect jdbc:hive2://localhost:10000
				.config("hive.metastore.uris", "thrift://localhost:9083") // 成功了！，先要启动metastore服务
				.enableHiveSupport()
				.getOrCreate();
		
		spark.sql("use classicmodels");
		spark.sql("show tables").show();
		//spark.sql("select * from employees limit 10").show();
		//spark.sql("select * from orderdetails limit 10").show();
		spark.sql("select "
				+ "b.customername,b.creditLimit,b.state,b.city,a.orderNumber,a.status, c.amount "
				+ "from orders a join customers b "
				+ "on a.customerNumber=b.customerNumber "
				+ "join payments c "
				+ "on a.customerNumber=c.customerNumber limit 10").show(); //完美
		//spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
		//spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");
		//spark.sql("SELECT * FROM src").show();
		

		
	}

}
