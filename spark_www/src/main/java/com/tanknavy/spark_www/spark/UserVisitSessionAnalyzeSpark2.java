package com.tanknavy.spark_www.spark;


import java.util.Date;
import java.util.Iterator;

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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import scala.Tuple2;

import com.alibaba.fastjson.JSONObject;
import com.tanknavy.spark_www.conf.ConfigurationManager;
import com.tanknavy.spark_www.constant.Constants;
import com.tanknavy.spark_www.dao.ISessionAggrStatDAO;
import com.tanknavy.spark_www.dao.ITaskDAO;
import com.tanknavy.spark_www.dao.impl.DAOFactory;
import com.tanknavy.spark_www.domain.SessionAggrStat;
import com.tanknavy.spark_www.domain.Task;
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

public class UserVisitSessionAnalyzeSpark2 {
	
	public static void main(String[] args) throws AnalysisException{
		// spark配置和上下文
		SparkConf conf = new SparkConf()
			.setAppName(Constants.SPARK_APP_NAME_SESSION)
			.setMaster("local[3]");
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
		args = new String[]{"11"};		//TEST:本应该用命令行参数读取
		/*
		 * INSERT INTO `www`.`task` (task_param)
		("{'startDate':['2019-01-01'],'endDate':['2019-12-31'],'startAge':['10'],'endAge':['50']，'cities':['city10','city20']}" );

		 */
		Long taskid = ParamUtils.getTaskIdFromArgs(args); 
		Task task = taskDAO.findById(taskid); //Task是JavaBean类, domain，和Task表完全对应
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		// 拿到指定时间范围的行文数据
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam); 
		//System.out.println(actionRDD.count()); //测试打印成功
		
		
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
		System.out.println(filteredSessionid2AggrInfoRDD.count()); //ok，范围过小容易导致没有数据
		/*
		System.out.println("filtered session-------------------------------------");
		for (Tuple2<String, String> tuple : filteredSessionid2AggrInfoRDD.take(5)){
			System.out.println(tuple._2);		
		}*/
		
		
		// 计算出各个时长、步长的统计占比，源数据已经在Accumulator中了，数据封装到domain调用DAO写入mysql
		// 注意：前面rdd没有action的话运算不会真正启动
		calculateAndPersistAggrStat(sessionAggStatAccumulator.value(), task.getTaskid());
		

		
		// 关闭spark上下文
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
						+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" //新增时长
						+ Constants.FIELD_STEP_LENGTH + "=" + stepLength; //新增步长
				
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
	
}
