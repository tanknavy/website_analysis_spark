package com.tanknavy.spark_www.spark;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.time.*;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.intervalLiteral_return;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.Tuple3;

import com.alibaba.fastjson.JSONObject;
import com.sun.tools.javac.resources.javac;
import com.tanknavy.spark_www.conf.ConfigurationManager;
import com.tanknavy.spark_www.constant.Constants;
import com.tanknavy.spark_www.dao.ISessionAggrStatDAO;
import com.tanknavy.spark_www.dao.ISessionDetail;
import com.tanknavy.spark_www.dao.ISessionRandomExtractDAO;
import com.tanknavy.spark_www.dao.ITaskDAO;
import com.tanknavy.spark_www.dao.ITop10CategoryDAO;
import com.tanknavy.spark_www.dao.ITop10SessionDAO;
import com.tanknavy.spark_www.dao.impl.DAOFactory;
import com.tanknavy.spark_www.domain.SessionAggrStat;
import com.tanknavy.spark_www.domain.SessionDetail;
import com.tanknavy.spark_www.domain.SessionRandomExtract;
import com.tanknavy.spark_www.domain.Task;
import com.tanknavy.spark_www.domain.Top10Category;
import com.tanknavy.spark_www.domain.Top10Session;
import com.tanknavy.spark_www.jdbc.JDBCHelper;
import com.tanknavy.spark_www.test.MockData;
import com.tanknavy.spark_www.util.DateUtils;
import com.tanknavy.spark_www.util.NumberUtils;
import com.tanknavy.spark_www.util.ParamUtils;
import com.tanknavy.spark_www.util.StringUtils;
import com.tanknavy.spark_www.util.ValidUtils;

/*
 * 用户访问session分析Spark作业
 * 
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * 
 * 用户的在J2EE上提交查询，查询条件被自动写到mySQL的Task表中，然后J2EE触发spark分析任务，spark从Task表中根据
 * 本次查询task id读取记录，获取对应的参数即task_parma栏位值(TEXT类型，保存为json格式)作为spark任务的条件
 */

public class UserVisitSessionAnalyzeSpark6 {
	
	private Logger log = LoggerFactory.getLogger(UserVisitSessionAnalyzeSpark6.class.getSimpleName());
	
	public static void main(String[] args) throws AnalysisException{
		// spark配置和上下文
		//Logger log = LoggerFactory.getLogger(UserVisitSessionAnalyzeSpark6.class.getSimpleName());
		Long beginTime = Instant.now().getEpochSecond();
		SparkConf conf = new SparkConf()
			.setAppName(Constants.SPARK_APP_NAME_SESSION)
			.setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//生产环境下从hive表读数据，测试从内存生成本地
		SQLContext sqlContext = getSQLContext(sc.sc()); //sc.sc()从java context取出对应的spark context
		
		//SparkSession spark=SparkSession.builder().appName("SparkReadHive").enableHiveSupport().getOrCreate();
		//spark.sparkContext().conf().set("spark.sql.warehouse.dir", "/user/hive/warehouse");
		//spark.sparkContext().conf().set("hive.metastore.uris", "thrift://localhost:10000");
		MockData.mock(sc, sqlContext);  // 准备数据源，或者本次内存，生产环境从Hive中
		
		// 查询本次指定的任务，获取任务的查询阐述
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		// 根据j2EE产生的任务id到mysql表中查找对应的参数，j2ee会触发spark-submit arg类似的shell脚本
		if(args.length == 0){args = new String[]{"11"};} 	//TEST:本应该用命令行参数读取

		/*
		 * INSERT INTO `www`.`task` (task_param)
		("{'startDate':['2019-01-01'],'endDate':['2019-12-31'],'startAge':['10'],'endAge':['50']，'cities':['city10','city20']}" );

		 */
		Long taskid = ParamUtils.getTaskIdFromArgs(args); 
		Task task = taskDAO.findById(taskid); //Task是JavaBean类, domain，和Task表完全对应
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		// 拿到指定时间范围的用户数据
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam); 
		//System.out.println(actionRDD.count()); //测试打印成功
		// 后续模块还需使用actionRDD这个用户明细数据
		JavaPairRDD<String, Row>  session2ActionRDD = getSession2ActionRDD(actionRDD); // <sessionid, Row>
		
		
		// 聚合操作：按照session_id进行groupByKey操作，此时数据的粒度是session,
		// 然后将session粒度的数据与用户信息数据进行join，得到完整用户信息，<sessionid, <sessionid, keyword,categoryId,  age,occupation,city,sex>>
		JavaPairRDD<String, String> sessionId2AggrInfoRDD = aggregateBySession(sqlContext, actionRDD);
		/*
		System.out.println(sessionId2AggrInfoRDD.count()); //测试打印，解决Row.getLong(6)可能为null出错后成功
		for (Tuple2<String, String> tuple : sessionId2AggrInfoRDD.take(5)){
			System.out.println(tuple._2);		
		}*/
		
		
		// 第一版：用户搜索条件过滤
		//JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSession(sessionId2AggrInfoRDD, taskParam);
		// 按照查找条件：年龄，职业，城市，性别，搜索词，点击，这几个条件过滤，编写自定义filter算子
		// 第二版：过滤时要同时统计时间,使用Accumulator
		
		/* 
		 * session聚合统计：统计访问时长，步长，各个区间的session总量占总session数量的比例
		 * 普通实现思路：遍历最初的actionRDD生成新的RDD，计算统计去更新自定义的Accumulator中对应的值，然后计算结果写入mySQL(解耦合，可维护)
		 * 
		 * 重构实现思路：不要再重新生成新的RDD，不要再遍历session数据
		 * 可以在之前session聚合时就计算访问时长和步长，
		 * Spark大型复杂项目原则：
		 * 1)尽量少生成新RDD，
		 * 2)尽量少进行RDD算子操作，
		 * 3)尽量少进行shuffle算子操作，比如groupByKey,reduceByKey,sortByKey,
		 *   会导致大量磁盘读写，严重降低性能，shuffle很容易导致数据倾斜，一旦数据倾斜，简直性能杀手
		 * 4)大数据项目性能第一，其次代码划分（解耦合，可维护），MapReduce,Hive,Spark
		 */
		@SuppressWarnings("deprecation")
		Accumulator<String> sessionAggStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator()); //初始化Accu
		// 注意；之前没有任何spark action动作的话，程序不会执行，累加器也会为空
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionId2AggrInfoRDD, taskParam, sessionAggStatAccumulator);
		//System.out.println(filteredSessionid2AggrInfoRDD.count()); //ok，范围过小容易导致没有数据
		
		//模块四：重构,生成公共的RDD,过滤后的session detail信息，可以大量用于后面的Join中
		JavaPairRDD<String, Row> sessionid2DetailRDD = getSessionid2DetailRDD(filteredSessionid2AggrInfoRDD,session2ActionRDD);
		
		
		/*
		System.out.println("filtered session-------------------------------------");
		for (Tuple2<String, String> tuple : filteredSessionid2AggrInfoRDD.take(5)){
			System.out.println(tuple._2);		
		}*/
		
		// 模块二：session抽样
		//第三版：每小时按比例随机抽样
		/* 第三版：session按比例随机抽样并写入mysql
		* session mapToPair按照时间(yyyy-mm-dd hh)作为key,countByKey计算每小时数量，然后除以总量得到占比,
		* 进而得到每小时应该抽样数量和具体sessionId
		* session gourpByKey(yyyy-mm-dd hh),遍历每组抽选上一步的sessionId
		* 已有数据：
		*/
		// 返回rdd并写入session_random_extract表中
		randomExtracSession(taskid, filteredSessionid2AggrInfoRDD, session2ActionRDD); // 同上也用相同的参数，但是每个都还需要另外用到
		
		
		// 计算出各个时长、步长的统计占比，源数据已经在Accumulator中了，数据封装到domain调用DAO写入mysql
		// 注意：前面rdd没有action的话运算不会真正启动
		// 写入session_aggr_stat
		calculateAndPersistAggrStat(sessionAggStatAccumulator.value(), task.getTaskid()); //Test ok
		
		/*
		 * 模块四：top10热门商品的top10最活跃session
		 * 需要过滤后的<>
		 */
		
		
		
		/*
		 * 模块三：top10热门商品以及排序
		 * input: filerted data, sessionAction
		 */
		
		//getTop10Category(task.getTaskid(),filteredSessionid2AggrInfoRDD, session2ActionRDD);
		List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(task.getTaskid(), sessionid2DetailRDD);
		
		
		/*
		 * 模块四：top10活跃用户
		 */
		getTop10Session(sc, task.getTaskid(), top10CategoryList, sessionid2DetailRDD);
		
		
		
		
		// 关闭spark上下文
		Long endTime = Instant.now().getEpochSecond();
		System.out.println("processed number of data records: " + session2ActionRDD.count());
		System.out.println("total runtime: "  + String.valueOf(endTime-beginTime) + " seconds");
		System.out.println("--------------completed sucessful--------------");
		sc.close();
	}



	/* 获取SQLContext
	 * 如果从本地测试环境，生成SQLContext对象
	 * 如果在生产环境，生成HiveContext对象
	 */
	// SQLContext从2.0开始被SparkSession取代
	private static SQLContext getSQLContext(SparkContext sc) {
	//private static SparkSession getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local){
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc); //HiveContext是SQLContext的子类
		}
	}
	
	
	// 指定时间范围的用户访问行为
	private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
		
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE); //避免硬编码，
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		// 从sql上下文中的临时表中读取，临时表就是从hive或者本地读取数据然后注册为spark临时表，这里对应mock_data中的user_visit_action
		String sql = "select * from global_temp.user_visit_action where date>='" + startDate + "' and date <='" + endDate + "'" ; //注意格式
		
		//DataFrame actionDF = sqlContext.sql(sql);
		Dataset<Row> actionDF = sqlContext.sql(sql);
		return actionDF.toJavaRDD(); // DataFrame -> RDD
	}
	
	/*
	 *  第四版：明细数据为了后面的join
	  <Row> -> <sessionId,Row>
	*/
	private static JavaPairRDD<String, Row> getSession2ActionRDD(JavaRDD<Row> actionRDD) {
		
		return actionRDD.mapToPair(new PairFunction<Row, String, Row>() { //匿名内部类
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				// TODO Auto-generated method stub
				 // Row对象就是表中有row,column的一条记录，栏位通过位置顺序读取
				return new Tuple2<String, Row>(row.getString(2), row); //表的第三个栏位就是sessionid的值
			}
		});
				
	}
	
	
	// 接上，actionRDD中一个row就是一行用户访问记录
	private static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext,JavaRDD<Row> actionRDD) {
		//(row) -> (sessionid,row)
		// 映射：mapToPair(PairFunction<Row, String, Row> f) //mapToPair需要一个PairFunction这个对象， PairFunction是个接口
		JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() { //匿名内部类
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Row>(row.getString(2), row); //row中和user_visit_action表格一行栏位一一对应
			}
		});
		
		// 映射以后分组, 一个sessionid对应一组行为
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey(); //Action后面多了个s
		
		// session分组后聚合，将session中所有搜索词和点击品类都聚合起来
		// 返回数据格式<userid,partAggrInfo(sessionId,searchKeywords,clickCategoryIds)>
		//JavaPairRDD<Long, String> sessionid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair( // 键由session变为user
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair( // 键由session变为user
				new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
					private static final long serialVersionUID = 1L;

			@Override
			// Iterable: Implementing this interface allows an object to be the target of the "for-each loop" statement
			public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> t) throws Exception {
				// TODO Auto-generated method stub
				String sessionid = t._1;
				// An iterator over a collection
				Iterator<Row> iterator = t._2.iterator(); //
				
				//聚合,一个session的相关行为聚合起来
				//A thread-safe, mutable sequence of characters. A string buffer is like a String, but can be modified
				StringBuffer searchKeywordsBuffer = new StringBuffer(""); // 拼接字符串,注意null和blank区别
				StringBuffer clickCategoryIDsBuffer = new StringBuffer("");
				Long userid = null;
				
				// 后续需要每个session的时长和步长，可以在这次遍历中一次完成，第二版中实现
				Date startTime = null;
				Date endTime = null;
				int  stepLength = 0;
				
				while(iterator.hasNext()){
					Row row = iterator.next();
					if(userid == null){ // session中第一条访问行为就拿到用户id
						userid = row.getLong(1);
					}
					String searchKeyword = row.getString(5); //row中和user_visit_action表格一行栏位一一对应

					//Long clickCategoryId = row.getLong(6); // 注意：为null怎么办时会报错java.lang.NullPointerException
					// https://spark.apache.org/docs/2.2.2/api/java/org/apache/spark/sql/Row.html
					//it is invalid to use the native primitive interface to retrieve a value that is null, 
					//instead a user must check isNullAt before attempting to retrieve a value that might be null.
					Long clickCategoryId =null; // 按照API要求Long类型如果可以为空先检查
					if (!row.isNullAt(6)){
						clickCategoryId = row.getLong(6);
					}
					
					
					//注意数据可能有null
					if (StringUtils.isNotEmpty(searchKeyword)) { //字符串不能为null也不能为空
						if(!searchKeywordsBuffer.toString().contains(searchKeyword)){ //buffer也不包含
							searchKeywordsBuffer.append(searchKeyword + ","); //最后一个逗号要去掉
						}
					}
					if (clickCategoryId != null) { //数值型
						if(!clickCategoryIDsBuffer.toString().contains(String.valueOf(clickCategoryId))){ // 转为字符串
							clickCategoryIDsBuffer.append(clickCategoryId + ","); //最后一个逗号要去掉
						}
					}
					
					//第二版：计算session时长(开始和结束时间)和步长
					//System.out.println("TEST:actionTime-------------------------------");
					//System.out.println(row.getString(4));
					Date actionTime = DateUtils.parseTime(row.getString(4));//表user_visit_action对应的action_time字符串栏位

					//Date actionTime = row.getDate(4); //采用java.sql.Date格式试试
					//System.out.println(actionTime); //TEST 运行中有时时间为空，或者.2019E这样奇怪的数字，可能前面时间格式不对
					if (startTime == null){
						startTime = actionTime;
					}
					if (endTime == null){
						endTime = actionTime;
					}
					if (actionTime.before(startTime)){
						startTime = actionTime;
					}
					if (actionTime.after(endTime)){
						endTime = actionTime;
					}
					
					stepLength ++; // session步长
					
				}
				
				// 到此，一个session中用户动作做，用户搜索词，点评商品类，聚合完成
				String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString()); //拼接时最后的逗号去掉
				String clickCategoryIds = StringUtils.trimComma(clickCategoryIDsBuffer.toString());
				// 第二版，计算session访问时长(单位是秒)
				long visitLength = (endTime.getTime() - startTime.getTime()) / 1000; //getTime返回毫秒，所以除以1000
				
				//返回数据格式：<sessionId,partAggrInfo>,考虑到后面要和user聚合，所有key应该是user而不是这里的sessionid
				// 这里增加一次转换返回<userId, partAggrInfo>
				// 两个字符串怎么拼接：key=value|key=value, 注意要判断是否为空(StringUtils.isNotEmpty(searchKeywords)? 1:"")
				String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" 
						//+ (StringUtils.isNotEmpty(searchKeywords)? Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" : "") //如果不为空
						//+ (StringUtils.isNotEmpty(clickCategoryIds)? Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds : "");
						+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" 
						+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
						+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" //第二版：新增时长
						+ Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" //新增步长
						+ Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime); 
						//第三版：新增起始时间，注意：之前是Date类型，为了做key,要转类型yyyy-MM-dd HH:mm:ss
					
				
				
				//return new Tuple2<String, String>(sessionid,partAggrInfo);
				return new Tuple2<Long, String>(userid,partAggrInfo);

			}

		});
		
		// 查询用户信息，为了和上面rdd(userid,partAggrInfo)聚合
		String sql = "select * from global_temp.user_info";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
		//测试一行
		//System.out.println(userInfoRDD.take(1).toString());
		//映射成<userId,Row>
		JavaPairRDD<Long, Row> userIdInfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Row> call(Row row) throws Exception {
				// TODO Auto-generated method stub
				// java.lang.String cannot be cast to java.lang.Long?
				return new Tuple2<Long,Row>(row.getLong(0),row);//<userid,Row>,从GlobalTempView临时表中第一个栏位就是userid
			}
		});
		
		// session粒度聚合数据和用户信息join得到<userid,<aggrInfo, userRDD>>
		JavaPairRDD<Long, Tuple2<String, Row>> useridFullInfoRDD = userid2PartAggrInfoRDD.join(userIdInfoRDD);
		
		// 对join起来的数据进行拼接，返回<sessionid, fullAggrInfo>格式,输入<userid,<(sessionid|keywords|categoryId),(userinfo)>>
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = useridFullInfoRDD.mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(
					Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
				// TODO Auto-generated method stub
				String partAggrInfo = tuple._2._1;
				Row userInfoRow = tuple._2._2;
				int age = userInfoRow.getInt(3);
				String professional = userInfoRow.getString(4);
				String city = userInfoRow.getString(5);
				String sex = userInfoRow.getString(6);
				
				String fullAggrInfo = partAggrInfo + "|" 
						+ Constants.FIELD_AGE + "=" + age + "|" 
						+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|" 
						+ Constants.FIELD_CITY + "=" + city + "|" 
						+ Constants.FIELD_SEX + "=" + sex;
				
				String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID); //转义
				// 返回完整的session拼接后的信息<sessionid,<sessionid, keyword,categoryId,  age,occupation,city,sex>>
				return new Tuple2<String, String>(sessionid, fullAggrInfo);
			}
		});
		
		
		return sessionid2FullAggrInfoRDD;
	}
	
	
	// 聚合数据按照使用者指定的筛选条件进行数据过滤，filter算子需要访问外部对象的，匿名内部类访问外面类的变量，需要声明为final类型
	// 第二版：增加时间统计，以及使用accumulator
	//private static JavaPairRDD<String, String> filterSession(JavaPairRDD<String, String> sessionid2FullAggrInfoRDD, final JSONObject taskParam){
	private static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2FullAggrInfoRDD, final JSONObject taskParam, final Accumulator<String> sessionAccu){
		
		// 解析用户查询参数并且拼接
		String startAge = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		// 为了使用工具类，以及性能优化
		String _parameter = (startAge !=null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				 + (endAge !=null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				 + (professionals !=null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				 + (cities !=null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				 + (sex !=null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				 + (keywords !=null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				 + (categoryIds !=null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");
				
		if (_parameter.endsWith("|")) { //假如后面有|
			_parameter = _parameter.substring(0, _parameter.length() -1); //最后字符不包括
		}
		
		final String parameter = _parameter;
		System.out.println("---TEST: input and parsed parameters-------------------------");
		System.out.println(taskParam.toJSONString());
		System.out.println(parameter);//TEST
		
		JavaPairRDD<String, String> filteredSession2AggrInfoRDD = sessionid2FullAggrInfoRDD.filter(new Function<Tuple2<String,String>, Boolean>() { // 匿名内部类
			private static final long serialVersionUID = 1L;
			// 匿名内部类的实现方法不能在出现方法块
			@Override
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				String aggrInfo = tuple._2;
				// 依次按照筛选条件过滤，age,job,city,sex, 最好使用工具类
				/*
				int age = Integer.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|", Constants.FIELD_AGE));
				String startAge = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE);
				String endAge = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE);
				
				if(startAge !=null && endAge !=null){
					if (age >= Integer.valueOf(startAge) && age<= Integer.valueOf(endAge)){
						return true;
					}
				}*/
				//TEST
				//System.out.println("---TEST: aggrInfo and parameter-------------------------");
				//System.out.println(aggrInfo);
				//System.out.println(parameter);
				// 这里引用了StringUtils.getFieldFromConcatString
				if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)){
					return false; //不满足条件马上返回
				}
				if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)){
					return false;
				}
				if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)){
					return false;
				}
				if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)){ //要么male要么female
					return false;
				}
				//session中随意一个搜索词在查询范围内，比如搜索(iPhone,cake),过滤条件(hotel,cake),只要有共同元素就true,
				if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)){ 
					return false;
				}
				if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)){
					return false;
				}
				
				// 前面都去掉不满足条件的多个过滤，即return false,走道这里才能是满足继续处理的session
				// 第二版：如果经过前面多个条件后依然满足程序能走到这里，都是需要保存的session, 
				// 现在session的访问时长和访问步长，进行统计和累计计数
				sessionAccu.add(Constants.SESSION_COUNT); // 总session数量，注意匿名内部类访问外部变量需要为final
				//System.out.println(sessionAccu.toString()); //TEST:ok,查看初始化好的session
				//System.out.println(aggrInfo);//TEST:ok
				long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
				long stepLength  = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
				calculateVistiLength(visitLength); //更新到累加器中
				calculateStepLength(stepLength); //更新到累加器中
				
				
				return true; //如果以上条件全部满足
			}
			
			// 匿名内部类Function中继续定义计算时长统计的方法，供call方法调用
			private void calculateVistiLength(long visitLength){ // 通过条件过滤的session到这里判断时长范围区间
				if (visitLength >= 1 && visitLength <=3){
					sessionAccu.add(Constants.TIME_PERIOD_1s_3s); // 对应的累加器中的区间+1
				} else if (visitLength >=4 && visitLength <=6){
					sessionAccu.add(Constants.TIME_PERIOD_4s_6s);
				} else if(visitLength >=7 && visitLength <= 9) {
					sessionAccu.add(Constants.TIME_PERIOD_7s_9s);  
				} else if(visitLength >=10 && visitLength <= 30) {
					sessionAccu.add(Constants.TIME_PERIOD_10s_30s);  
				} else if(visitLength > 30 && visitLength <= 60) {
					sessionAccu.add(Constants.TIME_PERIOD_30s_60s);  
				} else if(visitLength > 60 && visitLength <= 180) {
					sessionAccu.add(Constants.TIME_PERIOD_1m_3m);  
				} else if(visitLength > 180 && visitLength <= 600) {
					sessionAccu.add(Constants.TIME_PERIOD_3m_10m);  
				} else if(visitLength > 600 && visitLength <= 1800) {  
					sessionAccu.add(Constants.TIME_PERIOD_10m_30m);  
				} else if(visitLength > 1800) {
					sessionAccu.add(Constants.TIME_PERIOD_30m);  
				} 
			}
			
			//匿名内部类Function中继续定义计算步长的方法，供call方法调用
			private void calculateStepLength(long stepLength) {
				if(stepLength >= 1 && stepLength <= 3) {
					sessionAccu.add(Constants.STEP_PERIOD_1_3);  
				} else if(stepLength >= 4 && stepLength <= 6) {
					sessionAccu.add(Constants.STEP_PERIOD_4_6);  
				} else if(stepLength >= 7 && stepLength <= 9) {
					sessionAccu.add(Constants.STEP_PERIOD_7_9);  
				} else if(stepLength >= 10 && stepLength <= 30) {
					sessionAccu.add(Constants.STEP_PERIOD_10_30);  
				} else if(stepLength > 30 && stepLength <= 60) {
					sessionAccu.add(Constants.STEP_PERIOD_30_60);  
				} else if(stepLength > 60) {
					sessionAccu.add(Constants.STEP_PERIOD_60);    
				}
			}
			
		}); //end匿名类的call实现方法
		
		return filteredSession2AggrInfoRDD;
	}
	
	
	// 提取出来作为方法
	// 通过筛选条件的session的访问明细RDD: <session, aggrInfo> join <session, actionDetail> -> <session, actionDetail>
	private static JavaPairRDD<String, Row> getSessionid2DetailRDD(
			JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> session2ActionRDD){
		//符合条件的session的访问明细，<sessionid, row>
		JavaPairRDD<String, Row> sessionid2DetailRDD = filteredSessionid2AggrInfoRDD.join(session2ActionRDD)
				.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {
					private static final long serialVersionUID = 1L;
			public Tuple2<String, Row> call(
					Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Row>(tuple._1, tuple._2._2);
			}
		});
		return sessionid2DetailRDD;
		
	}
	
	
	
	/*
	 * 第三版：随机抽取session
	 * <sessionid,<aggrInfo,userInfo>> -> 
	 */
	private static void randomExtracSession(final long taskid, JavaPairRDD<String, String> sessionid2AggrInfoRDD, 
			JavaPairRDD<String, Row> session2ActionRDD) { // 新增一个rdd为了两个rdd的join
	
		// 计算每天每小时的session数量，输入的rdd:<sessionid,<aggrInfo,userInfo>>,
		// 需要的key是aggrInfo中的startTime<yyyy-MM-dd HH>,输出是<<yyyy-MM-dd HH>,sessionId>
		JavaPairRDD<String, String> time2SessionidRDD = sessionid2AggrInfoRDD.mapToPair(
				new PairFunction<Tuple2<String,String>, String, String>() {
			
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> tuple){
				String aggrInfo = tuple._2;
				String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
				// <yyyy-MM-dd HH:mm:ss> -><yyyy-MM-dd_HH, AggrInfo>,
				String dateHour = DateUtils.getDateHour(startTime); //工具类得到<yyyy-MM-dd_HH>
				// <<yyyy-MM-dd_HH>,aggrInfo>
				return new Tuple2<String, String>(dateHour, aggrInfo); //根据表session_random_extract需求
			}
		});
		
		/*
		 * 思考：每天每小时数量，然后计算每天每小时的session抽取索引
		 * 
		 */
		//Map<String, Object> countMap = time2SessionidRDD.countByKey(); //Long不能转为Object?
		Map<String, Long> countMap = time2SessionidRDD.countByKey();
		
		//下一步，按照比例随机抽取sessionid
		// <yyyy-MM-dd_HH,count> -> <yyyy-MM-dd,Map<HH,count>>方便使用
		Map<String, Map<String,Long>> dateHourCountMap = new HashMap<>();
		for (Map.Entry<String, Long> countEntry: countMap.entrySet()){ // map的遍历
			String dateHour = countEntry.getKey();
			long count = countEntry.getValue();
			//long count = Long.valueOf(countEntry.getValue()); //如果之前count是Object形式的话
			String date = dateHour.split("_")[0] ;
			String hour = dateHour.split("_")[1];
			
			Map<String, Long> hourCountMap = dateHourCountMap.get(date); //每天都是一个Map
			if (hourCountMap == null){ //每天的map是否存在
				hourCountMap = new HashMap<String, Long>();
				dateHourCountMap.put(date, hourCountMap); //当天按小时的map
			}
			hourCountMap.put(hour, count);
		}
		
		
		// 开始实现按时间比例随机抽取算法
		// 每天应该抽取多少个呢？这个日期时间范围总高抽取100个话，先按照天数，再按照小时数
		//long extractNumberPerDay = 100 / dateHourCountMap.size(); //每天应该多少个
		int extractNumberPerDay = 100 / dateHourCountMap.size(); //每天应该多少个 = 100个/多少天
		
		// <date,<hour,<session index>>>每天，每小时，抽取的sessionId索引；
		final Map<String, Map<String,List<Integer>>> dateHourNumber = new HashMap<>();
		Random random = new Random();
		
		//<day,<hour,count>>
		for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()){
			String date = dateHourCountEntry.getKey();
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
			
			//计算这一天session总数
			long dateSessionCount = 0L;
			for (long hourCount: hourCountMap.values()){
				dateSessionCount += hourCount; //当天总量
			}
			
			// 技巧：要不要在这套遍历中把存放抽样的每日map也顺便创建了，反正都是<date,<hour,value>>格式？
		    // 当天map是否存在，不存在就new
		    Map<String, List<Integer>> dateExtractMap = dateHourNumber.get(date);
		    if (dateExtractMap == null){
		    	dateExtractMap = new HashMap<String,List<Integer>>();
		    	// 新建的map记得放进去！！！
		    	dateHourNumber.put(date, dateExtractMap);//不然最大的map还是空
		    }
			
			
			//遍历每小时应该抽取的数量
			for (Map.Entry<String, Long> hourCountEntry: hourCountMap.entrySet()){
				String hour = hourCountEntry.getKey();
				Long hourCount = hourCountEntry.getValue(); //当前小时总量
				
				// 计算当前小时需要session数量，占当天总session数量的比例
				// 除数先double否则丢失精度,最终类型是long
			    //long hourExtractNumber = (long) (((double)count / (double)dateSessionCount) * extractNumberPerDay); 
			    int hourExtractNumber = (int) (((double)hourCount / (double)dateSessionCount) * extractNumberPerDay); 
				
			    // 检查一次，如果要抽取的数量超出现有数量
			    if (hourExtractNumber > hourCount){
			    	//hourExtractNumber = (int)hourCount;
			    	hourExtractNumber = hourCount.intValue(); // java1.8起
			    }
			    // 挑战：按照上面数量生成随机数
			    // 当前小时存放的随机数的list
			    
			    // 当天对应的map放到上面创建#593
			    
			    // 当天的这个小时的List是否存在，不存在就new
			    List<Integer> hourSessionIndex =  dateExtractMap.get(hour);
			    if (hourSessionIndex == null){
			    	hourSessionIndex = new ArrayList<>();
			    	// 新建的list记得放进去！！！
			    	dateExtractMap.put(hour, hourSessionIndex);
			    }
			    
			    for(int i=0;i< hourExtractNumber; i++){
			    	//当前小时总数hourCount,中抽取hourExtractNumber个
			    	int extractIndex = random.nextInt(hourCount.intValue()); // cannot cast from long to in
			    	// 如果索引重复反复再抽，直到唯一
			    	while(hourSessionIndex.contains(extractIndex)){
			    		extractIndex = random.nextInt(hourCount.intValue()); 
			    	}
				    // 拿到的索引放入对应List
				    hourSessionIndex.add(extractIndex);
			    }
			      
			}
			
		} //每小时应抽取的index拿到后，可以从aggrInfo中groupByKey,在从每个小时的gourp中根据index取得一行记录
		
		/*
		 * 每天每小时的session遍历
		 * 输入<<yyyy-MM-dd_HH>,aggrInfo>, <date,<time,<index>>>
		 */
		// <dateHour,Iter<session AggrInfo>>
		JavaPairRDD<String, Iterable<String>>  sessionbyHourRDD = time2SessionidRDD.groupByKey();
		
		// flatMap算子遍历每天每个小时的Iter<session AggrInfo>，获取dateHourNumber指定index的session aggrInfo
		// 抽取到的每条sesssion写入mysql表中
		// 返回抽取到的session返回新的rdd
		// 最后一步，抽取出来的sessionid去join他们访问行为明细数据
		
		// 第四版：为了抽取出来的sessionid去join用户信息，改flatMap为flatMapToPair
		//JavaRDD<String> sessionSampleRDD = sessionbyHourRDD.flatMap(new FlatMapFunction<Tuple2<String,Iterable<String>>, String>() {
		JavaPairRDD<String,String> sessionSampleRDD = sessionbyHourRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {
					private static final long serialVersionUID = 1L;

					public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple2)throws Exception {
						
						//List<String> extractSessionids = new ArrayList<>(); //只有sessionid
						List<Tuple2<String, String>> extractSessionids = new ArrayList<>(); // sessionid + AggrInfo
						//日期时间
						Iterator<String> iterator = tuple2._2.iterator(); // 当前小时的session清单
						String dateHour = tuple2._1;
						String date = tuple2._1.split("_")[0]; //yyyy-MM-dd_HH
						String hour = tuple2._1.split("_")[1];
						List<Integer> indexList = dateHourNumber.get(date).get(hour);
						//DAO接口 variable = DAO工厂.静态方法
						ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();
						
						int index=0; //迭代器元素索引
						while(iterator.hasNext()){
							String sessionAggrInfo = iterator.next();
							if(indexList.contains(index)){ //如果包含这个索引，保存到要返回的Iterator中
								String sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
								// 满足条件的session处理
								// 存入mysql,session聚合数据先封装到domain
								SessionRandomExtract sessionRandomExtract = new SessionRandomExtract(); // 数据库domain
								sessionRandomExtract.setTaskid(taskid); // 参考方法final变量
								sessionRandomExtract.setSessionid(sessionid);
								sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
								sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
								sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
								
								sessionRandomExtractDAO.insert(sessionRandomExtract); // 一个taskid可以分析很多session,这个taskid在表中不能是key,
								
								// session加入到list，返回到rdd
								//extractSessionids.add(sessionid); //当前sessionid,后续还需要加入session用户行为和信息
								extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid)); //当前sessionid,后续还需要加入session用户行为和信息
							}
							index ++;
						}
						
						/* 第二种挑选方法,直接按位置挑选，但是要知道Iterator的index,需要转成List或者Array
						List<Integer> indexList = dateHourNumber.get(date).get(hour); // 要抽取的index列表
						if( indexList != null && indexList.size() > 0){
							for (int i=0; i< indexList.size(); i++ ){
								
							}
						} */
						// 完成session的抽取和写入数据库表中
						// Tuple2<sessionid,sessionid>
						return extractSessionids.iterator(); //列表转成迭代器.java1.8
						// 第二版：<sessionId> join <session AggrInfo>

					}
		});

		
		/*
		 * 第四版：抽取session明细数据
		 * <session, <session, Row>>
		 */
		JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = sessionSampleRDD.join(session2ActionRDD);
		/*
		System.out.println("Test to delete: sample join action number-------------------------");
		System.out.println(sessionSampleRDD.count());
		System.out.println(sessionSampleRDD.take(1));
		System.out.println("Test to delete: after join-------------------------");
		System.out.println(extractSessionDetailRDD.count());
		System.out.println(extractSessionDetailRDD.take(1)); */
		
		//警告：spark就是要并行计算，假如设置了master = "local[3]",那么下面的List会产生三个，每个线程运行时保持一个
		//      但是最终的action时主线程的List没有数据还是空的，所以最终没有
		// 数据写入mysql中，先封装到domain中，
		// 性能调优，这里可以使用批量插入，还可以foreachPartition，因为并行进行！！！
		//extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>() {});
		// foreach,reduce,collect,count, take,save这个action会触发rdd执行
		/*
		final List<SessionDetail> sessionList = new ArrayList<>(); // 这是全局的，性能不好，没有分布式
		extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() { //rdd原封不动的过滤
			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				// 解析记录相关字段，放入domain，最后批量插入
				SessionDetail sessionDetail = new SessionDetail();
				
				// 找出对应表中需要的字段
				//String sessionid = tuple._1;
				Row row = tuple._2._2;
				
				// 放入domina...
				sessionDetail.setTaskid(taskid);
				//sessionDetail.setSessionid(sessionid);
				sessionDetail.setUserid(row.getLong(1));  
				sessionDetail.setSessionid(row.getString(2));  
				sessionDetail.setPageid(row.getLong(3));  
				sessionDetail.setActionTime(row.getString(4));
				if (!row.isNullAt(5)){ // 新版row中为空需要判断
					sessionDetail.setSearchKeyword(row.getString(5));
				}
				if (!row.isNullAt(6)){ // 新版row中为空需要判断
					sessionDetail.setClickCategoryId(row.getLong(6));
				}
				if (!row.isNullAt(7)){ // 新版row中为空需要判断
					sessionDetail.setClickProductId(row.getLong(7));
				}
				if (!row.isNullAt(8)){ // 新版row中为空需要判断
					sessionDetail.setOrderCategoryIds(row.getString(8)); 
				}
				if (!row.isNullAt(9)){ // 新版row中为空需要判断
					sessionDetail.setOrderProductIds(row.getString(9)); 
				}
				if (!row.isNullAt(10)){ // 新版row中为空需要判断
					sessionDetail.setPayCategoryIds(row.getString(10));
				}
				if (!row.isNullAt(11)){ // 新版row中为空需要判断
					sessionDetail.setPayProductIds(row.getString(11));
				} 
				
				// 方法一：在rdd中每个解析的记录执行一次insert，效能低下
				//ISessionDetail sessionDetailDAO = DAOFactory.getSessionDetail(); 
				//sessionDetailDAO.insert(sessionDetail);
				// 方法二：一次批量更新
				// domain添加到List
				sessionList.add(sessionDetail);
				//System.out.println(sessionList.size()); //显示sessionList列表有料
				
			}
		});
		System.out.println("Test: session detail number-------------------------");
		System.out.println(sessionList.size()); // sessionList到这里突然没料了？
		//全部row解析完成并放入参数列表，执行批量更新
		ISessionDetail sessionDetailDAO = DAOFactory.getSessionDetail(); 
		//接口如果只有insert方法，这样引用实现类特有的的方法不会出现,
		sessionDetailDAO.insertBatch(sessionList); //批量插入有返回受影响行数的值
		*/
		
		// 方法二，并行，每个Partition一个List，测试成功
		//final List<SessionDetail> sessionList = new ArrayList<>(); //Lis可以放在action RDD外面吗？测试是可以的
		extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>(){

			private static final long serialVersionUID = 1L;
			List<SessionDetail> sessionList = new ArrayList<>();
			public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterTuple) throws Exception {
				// 解析记录相关字段，放入domain，最后批量插入
				//List<SessionDetail> sessionList = new ArrayList<>();
				while(iterTuple.hasNext()){				
					// 这里输入是Iterator了，所以要while
					Row row = iterTuple.next()._2._2;
					//找出对应表中需要的字段放入domina...
					SessionDetail sessionDetail = new SessionDetail();
					sessionDetail.setTaskid(taskid);
					//sessionDetail.setSessionid(sessionid);
					sessionDetail.setUserid(row.getLong(1));  
					sessionDetail.setSessionid(row.getString(2));  
					sessionDetail.setPageid(row.getLong(3));  
					sessionDetail.setActionTime(row.getString(4));
					if (!row.isNullAt(5)){ // 新版row中为空需要判断
						sessionDetail.setSearchKeyword(row.getString(5));
					}
					if (!row.isNullAt(6)){ // 新版row中为空需要判断
						sessionDetail.setClickCategoryId(row.getLong(6));
					}
					if (!row.isNullAt(7)){ // 新版row中为空需要判断
						sessionDetail.setClickProductId(row.getLong(7));
					}
					if (!row.isNullAt(8)){ // 新版row中为空需要判断
						sessionDetail.setOrderCategoryIds(row.getString(8)); 
					}
					if (!row.isNullAt(9)){ // 新版row中为空需要判断
						sessionDetail.setOrderProductIds(row.getString(9)); 
					}
					if (!row.isNullAt(10)){ // 新版row中为空需要判断
						sessionDetail.setPayCategoryIds(row.getString(10));
					}
					if (!row.isNullAt(11)){ // 新版row中为空需要判断
						sessionDetail.setPayProductIds(row.getString(11));
					} 

					// 方法二：一次批量更新
					// domain添加到List
					sessionList.add(sessionDetail);
					//System.out.println(sessionList.size()); //显示sessionList列表有料，多线程
					
				} // while结束
				System.out.println("TEST：sessionList number--------------");
				System.out.println(sessionList.size()); //显示sessionList列表有料
				ISessionDetail sessionDetailDAO = DAOFactory.getSessionDetailDAO(); 
				//接口如果只有insert方法，这样引用实现类特有的的方法不会出现,
				sessionDetailDAO.insertBatch(sessionList); //批量插入有返回受影响行数的值
				
			}
			
		});
		
		/*
		System.out.println("Test: session detail number-------------------------");
		System.out.println(sessionList.size()); // sessionList到这里突然没料了？
		//全部row解析完成并放入参数列表，执行批量更新
		ISessionDetail sessionDetailDAO = DAOFactory.getSessionDetail(); 
		//接口如果只有insert方法，这样引用实现类特有的的方法不会出现,
		sessionDetailDAO.insertBatch(sessionList); //批量插入有返回受影响行数的值
		*/
		
		//return extractSessionDetailRDD;
		
	}
	
	
	/* 输入Accumulator.value的字符串
	 * 计算时长，步长并持久化到mySQL中
	 * 注意：在使用Accumulator前，没有任何spark actions算子，所以这里累加器是空
	 */
	private static void calculateAndPersistAggrStat(String value, Long taskid) {
		// 时长
		System.out.println("Test: Accumulator---------------");
		System.out.println(value.toString());// 前面如果没有触发，累加器为空
		long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
		long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));  
		long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));
		// 步长
		long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));
		
		// 百分比,使用BigDecimal四舍五入，注意值都是Long类型，在计算百分比的时候一定要转换为double,否则小数会直接抹平
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble((double) visit_length_1s_3s / (double) session_count, 2);
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble((double) visit_length_4s_6s / (double) session_count, 2);
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble((double) visit_length_7s_9s / (double) session_count, 2);
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble((double) visit_length_10s_30s / (double) session_count, 2);
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble((double) visit_length_30s_60s / (double) session_count, 2);
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble((double) visit_length_1m_3m / (double) session_count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble((double) visit_length_3m_10m / (double) session_count, 2);
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble((double) visit_length_10m_30m / (double) session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble((double) visit_length_30m / (double) session_count, 2);

		double step_length_1_3_ratio = NumberUtils.formatDouble((double) step_length_1_3 / (double) session_count, 2);
		double step_length_4_6_ratio = NumberUtils.formatDouble((double) step_length_4_6 / (double) session_count, 2);
		double step_length_7_9_ratio = NumberUtils.formatDouble((double) step_length_7_9 / (double) session_count, 2);
		double step_length_10_30_ratio = NumberUtils.formatDouble((double) step_length_10_30 / (double) session_count, 2);
		double step_length_30_60_ratio = NumberUtils.formatDouble((double) step_length_30_60 / (double) session_count, 2);
		double step_length_60_ratio = NumberUtils.formatDouble((double) step_length_60 / (double) session_count, 2);
		
		// session聚集数据写入mysql前封装到DAO
		SessionAggrStat sessionAggrStat = new SessionAggrStat(); //Domain
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_count);  
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);  
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);  
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);  
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);  
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);  
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio); 
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);  
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio); 
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);  
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);  
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);  
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);  
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);  
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);  
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);
		
		// domain准备准备好以后调用DAO实现的方法执行插入
		ISessionAggrStatDAO iSessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO(); //工厂方法
		iSessionAggrStatDAO.insert(sessionAggrStat);
		
	}
	
	
	/**
	 * 
	 * @param filteredSessionid2AggrInfoRDD, 按照每个session聚集后的数据
	 * @param session2ActionRDD， session的多个user action
	 * @return 
	 */
	//private static void getTop10Category(
	private static List<Tuple2<CategorySortKey, String>> getTop10Category( //top10 session需要
			long taskid,
			JavaPairRDD<String, Row> sessionid2DetailRDD) { //重构
			//JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
			//JavaPairRDD<String, Row> session2ActionRDD) {
		
		//符合条件的session的访问明细，<sessionid, row>, 新生成方法替换下面的功能
		/*
		JavaPairRDD<String, Row> sessionid2DetailRDD = filteredSessionid2AggrInfoRDD.join(session2ActionRDD)
				.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {
					private static final long serialVersionUID = 1L;
			public Tuple2<String, Row> call(
					Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Row>(tuple._1, tuple._2._2);
			}
		});
		*/
		//session访问(点击，下单，支付)过的所有商品id, 一个session可能有多个商品flatmap
		
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2DetailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {
		//JavaPairRDD<Long, Long> categoryidRDD = sessionid2DetailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

			private static final long serialVersionUID = 1L;

			public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple)throws Exception {
				// TODO Auto-generated method stub
				List<Tuple2<Long, Long>> list = new ArrayList<>();
				Row row = tuple._2;
				
				if (!row.isNullAt(6)){ // 新版row中为空需要判断
					list.add(new Tuple2<Long, Long>(row.getLong(6),row.getLong(6))); // 品类作为key为了后面的join
				}

				if (!row.isNullAt(8)){ // 下单的可能有多个商品
					String[] orderCategories = row.getString(8).split(",");
					for(String category: orderCategories){
						list.add(new Tuple2<Long, Long>(Long.valueOf(category),Long.valueOf(category)));
					}
				}

				if (!row.isNullAt(10)){ // 新版row中为空需要判断
					String[] payCategories = row.getString(10).split(",");
					for(String pay: payCategories){
						list.add(new Tuple2<Long, Long>(Long.valueOf(pay),Long.valueOf(pay)));
					}
				}
				
				return list.iterator();
			}
		});
		categoryidRDD = categoryidRDD.distinct(); //去重
		
		// 功能同上，采用java1.8匿名函数
		// session访问(点击，下单，支付)过的所有商品id, 一个session可能有多个商品flatMap，
		// 并行情况下List的使用
		
		/*
		List<Tuple2<Long, Long>> list = new ArrayList<>(); //放外面还是里面？
		
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2DetailRDD.flatMapToPair(tuple2 -> {
					// TODO Auto-generated method stub
					//List<Tuple2<Long, Long>> list = new ArrayList<>();
					Row row = tuple2._2;
			if (!row.isNullAt(6)) { // 新版row中为空需要判断
				list.add(new Tuple2<Long, Long>(row.getLong(6), row.getLong(6))); // 品类作为key为了后面的join
			}

			if (!row.isNullAt(8)) { // 下单的可能有多个商品
				String[] orderCategories = row.getString(8).split(",");
				for (String category : orderCategories) {
					list.add(new Tuple2<Long, Long>(Long.valueOf(category),Long.valueOf(category)));
				}
			}

			if (!row.isNullAt(10)) { // 新版row中为空需要判断
				String[] payCategories = row.getString(10).split(",");
				for (String pay : payCategories) {
					list.add(new Tuple2<Long, Long>(Long.valueOf(pay), Long.valueOf(pay)));
				}
			}
			
			//bug: 不同session会有相同category,在后面join中，这里category必须要唯一，否则后面top10都相同了
			
			return list.iterator();
		});
		categoryidRDD = categoryidRDD.distinct();
		*/
		
		/*
		// bug修复，手动笨办法，没有效果,一是因为这是多线程的，放在call里面没有效果和意义，二是放外面没有效果更没有scable
		List<Tuple2<Long, Long>> list = new ArrayList<>();
		Set<Long> set = new HashSet<>(); //为了去重，手动方法
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2DetailRDD.flatMapToPair(tuple2 -> { // flatMap里面当然不能去重
					// TODO Auto-generated method stub
			//Set<Long> set = new HashSet<>(); //相当于在call里面
			//List<Tuple2<Long, Long>> list = new ArrayList<>();
			Row row = tuple2._2;
			if (!row.isNullAt(6)) { // 新版row中为空需要判断
				if ( !set.contains(row.getLong(6))){
					set.add(row.getLong(6));
					list.add(new Tuple2<Long, Long>(row.getLong(6), row.getLong(6))); // 品类作为key为了后面的join
				}
				//list.add(new Tuple2<Long, Long>(row.getLong(6), row.getLong(6))); // 品类作为key为了后面的join
			}

			if (!row.isNullAt(8)) { // 下单的可能有多个商品
				String[] orderCategories = row.getString(8).split(",");
				for (String category : orderCategories) {
					if ( !set.contains(Long.valueOf(category))){
						set.add(Long.valueOf(category));
						list.add(new Tuple2<Long, Long>(Long.valueOf(category),Long.valueOf(category))); // 品类作为key为了后面的join
					}
					//list.add(new Tuple2<Long, Long>(Long.valueOf(category),Long.valueOf(category)));
				}
			}

			if (!row.isNullAt(10)) { // 新版row中为空需要判断
				String[] payCategories = row.getString(10).split(",");
				for (String pay : payCategories) {
					if ( !set.contains(Long.valueOf(pay))){
						set.add(Long.valueOf(pay));
						list.add(new Tuple2<Long, Long>(Long.valueOf(pay), Long.valueOf(pay))); // 品类作为key为了后面的join
					}
					//list.add(new Tuple2<Long, Long>(Long.valueOf(pay), Long.valueOf(pay)));
				}
			}

			// bug: 不同session会有相同category,在后面join中，这里category必须要唯一，否则后面top10都相同了
			System.out.println("TEST: set --------------------");
			for(Long value:set){
				System.out.println(value);
			}
			
			return list.iterator();
		});
		*/
		

		// 计算各品类点击，下单，支付的次数, 另外的方法
		// 所有访问过的品类categories<category,category> 和 以下三个rdd(category,count) join
		// 重构时才发现：session2ActionRDD 代表所有全部session行为，sessionid2DetailRDD代表过滤后的session行为
		JavaPairRDD<Long, Long> clickCategory2CountRDD = getClickCategory2CountRDD(sessionid2DetailRDD);
		JavaPairRDD<Long, Long> orderategory2CountRD = getOrderategory2CountRDD(sessionid2DetailRDD);
		JavaPairRDD<Long, Long> payCategory2CountRDD = getPayCategory2CountRDD(sessionid2DetailRDD);
	
		// 4个RDD的join
		// Category是所有被访问过的，其他3个RDD可能会被包含在category RDD中，所以这里只能用leftjoin
		// 期待结果：JavaPairRDD<Long, clickCount, orderCount, payCount>
		// Tuple2<category,"click=count|order=count|pay=count">
		JavaPairRDD<Long, String> categoryAcessRDD = joinCategoryAndCount(categoryidRDD,clickCategory2CountRDD,orderategory2CountRD,payCategory2CountRDD);
		//.leftOuterJoin(orderategory2CountRD).leftOuterJoin(payCategory2CountRDD);
		
		
		// 目标：二次排序求topN: <category,"click=count|order=count|pay=count">
		// 开发二次排序的key CategorySortKey
		
		
		// 将数据<category,"click=count|order=count|pay=count">映射成<sortKey,info>格式的RDD
		JavaPairRDD<CategorySortKey, String> sortKey2CountRDD = categoryAcessRDD.mapToPair(
				new PairFunction<Tuple2<Long,String>, CategorySortKey, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple2)throws Exception {

				String countInfo = tuple2._2; // <"click=count|order=count|pay=count">
				long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
				long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
				long payCount   = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
				
				CategorySortKey categorySortKey = new CategorySortKey(clickCount,orderCount,payCount); //构造方法

				return new Tuple2<CategorySortKey, String>(categorySortKey, countInfo);
			}
		});
	
		
		// sortbyKey执行二次排序
		//java.io.NotSerializableException
		JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2CountRDD.sortByKey(false);//降序
		
		
		// take(10)拿到前十个热门品类并写入mysql
		List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
		
		List<Top10Category> paramList = new ArrayList<>();
		for (Tuple2<CategorySortKey, String> tuple: top10CategoryList){
			String countInfo = tuple._2;
			long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FEILD_CATEGORY_ID));
			long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
			long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
			long payCount   = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
			
			Top10Category top10Category = new Top10Category();
			top10Category.setTaskid(taskid);
			top10Category.setCategoryid(categoryid);
			top10Category.setClickCount(clickCount);
			top10Category.setOrderCount(orderCount);
			top10Category.setPayCount(payCount);
			paramList.add(top10Category);
		}
		ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
		top10CategoryDAO.insertBatch(paramList);
		
		return top10CategoryList;
		
	}
	
	
	private static JavaPairRDD<Long, Long> getClickCategory2CountRDD(JavaPairRDD<String, Row> session2ActionRDD){
		// 从session action明细中过滤有click品类
				JavaPairRDD<String, Row> clickActionRDD = session2ActionRDD.filter(new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
						Row row = tuple2._2;
						//row.get(6) //使用get而不是getLong判断是否为空，不能会直接用row.getLong，在Null的情况下getLong会返回0,导致category出现大量
						return !row.isNullAt(6); //
					}
				});
				
				
				JavaPairRDD<Long, Long> filterClickActionRDD = clickActionRDD.mapToPair(new PairFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;
					public Tuple2<Long, Long> call(Tuple2<String, Row> tuple)throws Exception {
						Row row = tuple._2();
						return (new Tuple2<>(row.getLong(6), 1L)); // (key,1)出现一次 
					}
				});
				
				JavaPairRDD<Long, Long> clickCategory2CountRDD = filterClickActionRDD.reduceByKey((x,y)-> x+y); // lambda表达式
				
				return clickCategory2CountRDD;
	}
	
	
	
	private static JavaPairRDD<Long, Long> getOrderategory2CountRDD(JavaPairRDD<String, Row> session2ActionRDD){
		// 从session action明细中过滤有order品类
				JavaPairRDD<String, Row> orderActionRDD = session2ActionRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
							private static final long serialVersionUID = 1L;
							public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
								Row row = tuple2._2;
								return !row.isNullAt(8);
							}
						});

				JavaPairRDD<Long, Long> filterOrderActionRDD = orderActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {
					private static final long serialVersionUID = 1L;

					public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple)throws Exception {
						List<Tuple2<Long, Long>> list = new ArrayList<>();
						Row row = tuple._2();
						String[] orderCategories = row.getString(8).split(",");
						for (String category : orderCategories) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(category),1L));
						}
						return list.iterator();
					}
				});

				JavaPairRDD<Long, Long> orderCategory2CountRDD = filterOrderActionRDD.reduceByKey((x, y) -> x + y); // lambda表达式
				
				return orderCategory2CountRDD;
	}
	
	private static JavaPairRDD<Long, Long> getPayCategory2CountRDD(JavaPairRDD<String, Row> session2ActionRDD){
		// 从session action明细中过滤有pay品类
				JavaPairRDD<String, Row> payActionRDD = session2ActionRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
							private static final long serialVersionUID = 1L;
							public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
								Row row = tuple2._2;
								return !row.isNullAt(10);
							}
						});

				JavaPairRDD<Long, Long> filterPayActionRDD = payActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {
					private static final long serialVersionUID = 1L;

					public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple)throws Exception {
						List<Tuple2<Long, Long>> list = new ArrayList<>();
						Row row = tuple._2();
						String[] payCategories = row.getString(10).split(",");
						for (String category : payCategories) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(category),1L));
						}
						return list.iterator();
					}
				});

				JavaPairRDD<Long, Long> payCategory2CountRDD = filterPayActionRDD.reduceByKey((x, y) -> x + y); // lambda表达式
				
				return payCategory2CountRDD;
	}
	

	/**
	 * 	4个 RDD left join方法
		期待结果：JavaPairRDD<Long, clickCount, orderCount, payCount>
		实际： JavaPairRDD<Long,String>
	 * @param categoryidRDD
	 * @param clickCategory2CountRDD
	 * @param orderategory2CountRD
	 * @param payCategory2CountRDD
	 * @return
	 */
	private static JavaPairRDD<Long, String> joinCategoryAndCount(
			JavaPairRDD<Long, Long> categoryidRDD,
			JavaPairRDD<Long, Long> clickCategory2CountRDD,
			JavaPairRDD<Long, Long> orderategory2CountRD,
			JavaPairRDD<Long, Long> payCategory2CountRDD){
		
		// Optional Like java.util.Optional in Java 8, scala.Option in Scala, and com.google.common.base.Optional
		// 右边可能为空，所以要用Optional类型
		// 可以不拼字符串，改用 Tuple2<category,Tuple3<click,order,pay>> ?
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryidRDD.leftOuterJoin(clickCategory2CountRDD);
		JavaPairRDD<Long, String> tmpClickMapRDD = tmpJoinRDD.mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<Long,Optional<Long>>>, Long, String>() {
			@Override
			public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple2)throws Exception {
				Long categoryid = tuple2._1;
				Optional<Long> clickCountOptional = tuple2._2._2;
				long clickCount = 0L;
				if (clickCountOptional.isPresent()){ //如果optional有值
					clickCount = clickCountOptional.get();
				}
				String value = Constants.FEILD_CATEGORY_ID + "=" + categoryid + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount;
				return new Tuple2<>(categoryid, value);
			}
		});
		
		JavaPairRDD<Long, Tuple2<String, Optional<Long>>> tmpOrderJoinRDD = tmpClickMapRDD.leftOuterJoin(orderategory2CountRD);
		JavaPairRDD<Long, String> tmpOrderMapRDD = tmpOrderJoinRDD.mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple2)throws Exception {
						Long categoryid = tuple2._1;
						Optional<Long> orderCountOptional = tuple2._2._2;
						long orderCount = 0L;
						if (orderCountOptional.isPresent()){ //如果optional有值
							orderCount = orderCountOptional.get();
						}
						String value = tuple2._2._1 + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
						
						return new Tuple2<>(categoryid, value);
					}
		});
		
		JavaPairRDD<Long, Tuple2<String, Optional<Long>>> tmpPayJoinRDD = tmpOrderMapRDD.leftOuterJoin(payCategory2CountRDD);
		JavaPairRDD<Long, String> tmpPayMapRDD = tmpPayJoinRDD.mapToPair(tuple2 ->{
			Long categoryid = tuple2._1;
			Optional<Long> payCountOptional = tuple2._2._2;
			long payCount = 0L;
			if (payCountOptional.isPresent()){ //如果optional有值
				payCount = payCountOptional.get();
			}
			String value = tuple2._2._1 + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;
			return new Tuple2<>(categoryid, value);
		}); // lambda
		
		
		return tmpPayMapRDD; //<category,"click=count|order=count|pay=count">
		
	}
	
	
	/**
	 * 获取top10 category对应的top10活跃session
	 * @param taskid
	 * @param top10CategoryList  <categoryKey,<category+countInfo>>
	 * @param 过滤后sessionid2DetailRDD:<sessionid,action>
	 */
	private static void getTop10Session(JavaSparkContext sc, long taskid, 
			List<Tuple2<CategorySortKey, String>> top10CategoryList, JavaPairRDD<String, Row> sessionid2DetailRDD) {
		// <categoryKey,<category+countInfo>> -> <categoryId,categoryId>
		List<Tuple2<Long, Long>>  top10categoryIdList = new ArrayList<>();
		for(Tuple2<CategorySortKey, String> tuple : top10CategoryList){
			Long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(tuple._2, "\\|", Constants.FEILD_CATEGORY_ID));
			top10categoryIdList.add(new Tuple2<Long, Long>(categoryId,categoryId));
		}
		// list-> rdd
		JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10categoryIdList);
		
		// <session, Iterable<action>
		JavaPairRDD<String, Iterable<Row>> session2DetailGroupRDD = sessionid2DetailRDD.groupByKey();
		
		// category分组，每组每个session
		//<sessionid,action> -><category, sessionId, count>, 一个session可能有多个category
		JavaPairRDD<Long, String> categoryId2SessionCountRDD = session2DetailGroupRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
					private static final long serialVersionUID = 1L;
					
					public Iterator<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
						String sessionId = tuple._1;
						Iterable<Row> Rows = tuple._2;
						
						Map<Long, Long> categoryCountInSessionMap = new HashMap<>(); // session内计数
						// 每个session中每个品类的点击次数
						for(Row row: Rows){ // 每一个row中间
							if (!row.isNullAt(6)){
							//if (row.get(6) != null){
								Long categoryId = row.getLong(6);
								
								Long count = categoryCountInSessionMap.get(categoryId);
								if( count == null){
									count = 0L;
								}
								count ++;
								categoryCountInSessionMap.put(categoryId, count);
							}
						}
						
						// 一个session处理完成后，希望返回<categoryId,sessionId:count>
						List<Tuple2<Long, String>> categorySessionList = new ArrayList<>();
						for(Map.Entry<Long, Long> map:categoryCountInSessionMap.entrySet()){
							Long   categoryId = map.getValue();
							String categoryCount = sessionId + "," + map.getValue();
							categorySessionList.add(new Tuple2<Long, String>(categoryId, categoryCount));
						}
						
						return categorySessionList.iterator();
					}
				
		}); // <category, sessionId, count>
		
		//<category,category> join <category, sessionId, count>
		// 得到top10Cateogy被session点击的信息
		JavaPairRDD<Long, String> top10CategorySessionRDD = top10CategoryIdRDD.join(categoryId2SessionCountRDD)
				.mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,String>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> t)
							throws Exception {
						
						return new Tuple2<Long, String>(t._1, t._2._2); // <category, <sessionId,count>>
					}
				});
		
		// 分组topN, 组内用窗口函数还是用treeMap?
		//按照category分组，组内就是<sessionId, count>列表, 组内按照count大小找到top10session
		// session明细数据：sessionid2DetailRDD
		
		// 多线程怎么处理？
		JavaPairRDD<String, String> final10 = top10CategorySessionRDD.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String, String>() {
			private static final long serialVersionUID = 1L;
			public Iterator<Tuple2<String, String>> call(
					Tuple2<Long, Iterable<String>> t) throws Exception {
				long  categoryId = t._1;
				Iterator<String> iter = t._2.iterator(); // <sessionId,count>列表
				// treeMap 取topN?
				TreeMap<Long, String> top10Map = new TreeMap<>();
				// 数组也可以
				String[] top10Arr = new String[10];
				
				while(iter.hasNext()){
					String sessionCount = iter.next();
					String sessionId = sessionCount.split(",")[0];
					long   count = Long.valueOf(sessionCount.split(",")[1]);
					// topN treeMap算法
					top10Map.put(count, sessionId);
					if (top10Map.size() > 10){
						top10Map.pollFirstEntry(); //移除最小键的k/v
					}
					
					//debug: 数组指定为10个，假如实际没有10个怎么办？
					// topN 数组算法：每次插入第一位，其余小的往后移，这样就从大到小10个
					for(int i=0; i<top10Arr.length;i++){ // 10次循环很重要
						if(top10Arr[i] == null){
							top10Arr[i] = sessionCount;
							break; //如果还有空位，放入后直接返回
						}else {
							long _count = Long.valueOf(top10Arr[i].split(",")[1]); 
							if(count > _count){ //空位直接放入
								for(int j=top10Arr.length -1; j>i; j--){ // 后面依次比较并调整位置
									top10Arr[j] = top10Arr[j-1]; //依次往后移动一位
								}
								top10Arr[i] = sessionCount; //腾出位置放入更大的新来的
								break; //如果大于处理完成了，如果小于了，没有任何处理继续进入外层循环，i--,和后面的再次比较
							}
						}
					} // 数组topN;
					
				}
			    //每个category对应的top10 session已经准备好
				// 数组是sessionCount[], treeMap是TreeMap<Long, String>
				List<Tuple2<String, String>> top10 = new ArrayList<>();
				
				//System.out.println("TEST: Array to keep top N-------------------------");
				// 测试数组topN的效果
				
				for(int i=0;i<top10Arr.length;i++){
					System.out.println(top10Arr[i]);
				}
				
			    // 写入数据库, 使用batch插入性能更好
				List<Top10Session> paramsList = new ArrayList<>();
				
				/*
				for(Map.Entry<Long, String> map: top10Map.entrySet()){
			    	top10.add(new Tuple2<String, String>(map.getValue(), String.valueOf(map.getKey())));
			    	
			    	Top10Session session = new Top10Session();
					
			    	//sql插入参数
					session.setTaskid(taskid);
					session.setCategoryid(categoryId);
					session.setSessionid(map.getValue());
					session.setClickCount(map.getKey());
					paramsList.add(session); 
			    	// 测试treeMap的效果
			    	//System.out.println(map.getValue() + "," +  String.valueOf(map.getKey()));
			    }
			    */

				
				for(String sessionCount:top10Arr){ // 
					// 数组长度为10，但有些可能没有10个
					if (sessionCount != null){
						Top10Session session = new Top10Session();
					
						session.setTaskid(taskid);
						session.setCategoryid(categoryId);
						session.setSessionid(sessionCount.split(",")[0]);
						session.setClickCount(Long.valueOf(sessionCount.split(",")[1]));
						paramsList.add(session); //sql插入参数
					
						top10.add(new Tuple2<String, String>(sessionCount.split(",")[0], sessionCount.split(",")[1])); //加入rdd返回
					
						//System.out.println("TEST: top N category's top N session-------------------------");
						//System.out.println(session.getSessionid());
					}
				}
				

				ITop10SessionDAO sessionDAO = DAOFactory.getTop10SessionDAO();
				sessionDAO.insertBatch(paramsList);
				
				return top10.iterator(); // 一个品类的topN session
			}
		});
		//TEST
		//final10.collect();
		//获取top10活跃用户的明细并写入mysql
		
		
		JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = final10.join(sessionid2DetailRDD);
		
		System.out.println("Test to delete: final10 session join action detail-------------------------");
		System.out.println("final10.count:" + final10.count());
		//System.out.println(final10.take(1));
		System.out.println("sessionid2DetailRDD.count:" + sessionid2DetailRDD.count());
		//System.out.println(sessionid2DetailRDD.take(1));
		System.out.println("Test to delete: top N session List-------------------------");
		System.out.println("extractSessionDetailRDD.count:" + extractSessionDetailRDD.count());
		//System.out.println(extractSessionDetailRDD.take(1));
		
		
		//警告：spark就是要并行计算，假如设置了master = "local[3]",那么下面的List会产生三个，每个线程运行时保持一个
		//      但是最终的action时主线程的List没有数据还是空的，所以最终没有
		// 数据写入mysql中，先封装到domain中，
		// 性能调优，这里可以使用批量插入，还可以foreachPartition，因为并行进行！！！
		//extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>() {});
		// foreach,reduce,collect,count, take,save这个action会触发rdd执行
		
		/*
		final List<SessionDetail> sessionList = new ArrayList<>(); // 这是全局的，性能不好，没有分布式，并且结果还为空在foreach后
		extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() { //rdd原封不动的过滤
			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				// 解析记录相关字段，放入domain，最后批量插入
				SessionDetail sessionDetail = new SessionDetail();
				//List<SessionDetail> sessionList = new ArrayList<>(); // 在foreach方法中
				// 找出对应表中需要的字段
				//String sessionid = tuple._1;
				Row row = tuple._2._2;
				
				// 放入domina...
				sessionDetail.setTaskid(taskid);
				//sessionDetail.setSessionid(sessionid);
				sessionDetail.setUserid(row.getLong(1));  
				sessionDetail.setSessionid(row.getString(2));  
				sessionDetail.setPageid(row.getLong(3));  
				sessionDetail.setActionTime(row.getString(4));
				if (!row.isNullAt(5)){ // 新版row中为空需要判断
					sessionDetail.setSearchKeyword(row.getString(5));
				}
				if (!row.isNullAt(6)){ // 新版row中为空需要判断
					sessionDetail.setClickCategoryId(row.getLong(6));
				}
				if (!row.isNullAt(7)){ // 新版row中为空需要判断
					sessionDetail.setClickProductId(row.getLong(7));
				}
				if (!row.isNullAt(8)){ // 新版row中为空需要判断
					sessionDetail.setOrderCategoryIds(row.getString(8)); 
				}
				if (!row.isNullAt(9)){ // 新版row中为空需要判断
					sessionDetail.setOrderProductIds(row.getString(9)); 
				}
				if (!row.isNullAt(10)){ // 新版row中为空需要判断
					sessionDetail.setPayCategoryIds(row.getString(10));
				}
				if (!row.isNullAt(11)){ // 新版row中为空需要判断
					sessionDetail.setPayProductIds(row.getString(11));
				} 
				
				// 方法一：在rdd中每个解析的记录执行一次insert，效能低下
				//ISessionDetail sessionDetailDAO = DAOFactory.getSessionDetail(); 
				//sessionDetailDAO.insert(sessionDetail);
				// 方法二：一次批量更新
				// domain添加到List
				sessionList.add(sessionDetail);
				System.out.println("Test: top 10 session detail count：" + sessionList.size()); //显示sessionList每个都是1,因为在foreach里面
				
			}
		});
		System.out.println("Test: top 10 session detail count: " + sessionList.size());// sessionList到这里突然没料了？
		//全部row解析完成并放入参数列表，执行批量更新
		ISessionDetail sessionDetailDAO = DAOFactory.getSessionDetailDAO(); 
		//接口如果只有insert方法，这样引用实现类特有的的方法不会出现,
		int[] res = sessionDetailDAO.insertBatch(sessionList); //批量插入有返回受影响行数的值
		System.out.println("number of insert query: " + res.length);
		*/
		
		
		// 方法二，并行，每个Partition一个List，测试成功
		//final List<SessionDetail> sessionList = new ArrayList<>(); //Lis可以放在action RDD外面吗？测试是可以的
		// 每个线程维护一个独立的List
		List<SessionDetail> sessionList = new ArrayList<>();
		extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>(){

			private static final long serialVersionUID = 1L;
			//List<SessionDetail> sessionList = new ArrayList<>();

			public void call(
					Iterator<Tuple2<String, Tuple2<String, Row>>> iterTuple)throws Exception {
						// 解析记录相关字段，放入domain，最后批量插入
						// List<SessionDetail> sessionList = new ArrayList<>();
						while (iterTuple.hasNext()) {
							// 这里输入是Iterator了，所以要while
							Row row = iterTuple.next()._2._2;
							// 找出对应表中需要的字段放入domina...
							SessionDetail sessionDetail = new SessionDetail();
							sessionDetail.setTaskid(taskid);
							// sessionDetail.setSessionid(sessionid);
							sessionDetail.setUserid(row.getLong(1));
							sessionDetail.setSessionid(row.getString(2));
							sessionDetail.setPageid(row.getLong(3));
							sessionDetail.setActionTime(row.getString(4));
							if (!row.isNullAt(5)) { // 新版row中为空需要判断
								sessionDetail.setSearchKeyword(row.getString(5));
							}
							if (!row.isNullAt(6)) { // 新版row中为空需要判断
								sessionDetail.setClickCategoryId(row.getLong(6));
							}
							if (!row.isNullAt(7)) { // 新版row中为空需要判断
								sessionDetail.setClickProductId(row.getLong(7));
							}
							if (!row.isNullAt(8)) { // 新版row中为空需要判断
								sessionDetail.setOrderCategoryIds(row.getString(8));
							}
							if (!row.isNullAt(9)) { // 新版row中为空需要判断
								sessionDetail.setOrderProductIds(row.getString(9));
							}
							if (!row.isNullAt(10)) { // 新版row中为空需要判断
								sessionDetail.setPayCategoryIds(row.getString(10));
							}
							if (!row.isNullAt(11)) { // 新版row中为空需要判断
								sessionDetail.setPayProductIds(row.getString(11));
							}

							// 方法二：一次批量更新
							// domain添加到List
							sessionList.add(sessionDetail);
							// System.out.println(sessionList.size());
							// //显示sessionList列表有料，多线程

						} // while结束
						//System.out.println("TEST：topN category's top n sessionList number--------------");
						System.out.println("Test: top 10 session detail count：" + sessionList.size()); // 显示sessionList列表有料
						ISessionDetail sessionDetailDAO = DAOFactory.getSessionDetailDAO();
						// 接口如果只有insert方法，这样引用实现类特有的的方法不会出现,
						int[] res = sessionDetailDAO.insertBatch(sessionList); // 批量插入有返回受影响行数的值
						System.out.println("number of insert query: "+ res.length);
					}
				});
		
	}

}
