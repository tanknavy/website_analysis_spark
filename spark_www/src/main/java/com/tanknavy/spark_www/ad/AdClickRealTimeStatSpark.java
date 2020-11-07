package com.tanknavy.spark_www.ad;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.consumer.Blacklist;
import kafka.serializer.StringDecoder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

//import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.reflect.internal.Trees.Return;
import scala.tools.nsc.typechecker.StructuredTypeStrings;

import com.tanknavy.spark_www.conf.ConfigurationManager;
import com.tanknavy.spark_www.constant.Constants;
import com.tanknavy.spark_www.dao.IAdBlacklistDAO;
import com.tanknavy.spark_www.dao.IAdClickTrendDAO;
import com.tanknavy.spark_www.dao.IAdProvinceTop3DAO;
import com.tanknavy.spark_www.dao.IAdStatDAO;
import com.tanknavy.spark_www.dao.IAdUserClickCountDAO;
import com.tanknavy.spark_www.dao.impl.DAOFactory;
import com.tanknavy.spark_www.domain.AdBlacklist;
import com.tanknavy.spark_www.domain.AdClickTrend;
import com.tanknavy.spark_www.domain.AdProvinceTop3;
import com.tanknavy.spark_www.domain.AdStat;
import com.tanknavy.spark_www.domain.AdUserClickCount;
import com.tanknavy.spark_www.jdbc.JDBCHelper;
import com.tanknavy.spark_www.spark.UserVisitSessionAnalyzeSpark6;
import com.tanknavy.spark_www.util.DateUtils;

//广告点击流量实时统计
//关键点：transform 作用于每个rdd，updateStateByKey全局更新， rdd->row, sqlContext

// 容错：driver自动重启，全局状态使用checkpoint，接受日志预写入打开
// 性能：
// 1)并行化数据接收，处理多个topic有效;2)增加block数量，增加batch rdd的partition数量，增加处理并行度
// receiver默认每block interval 200ms毫秒间隔的数据收集为一个block，然后将制定batch interval时间间隔内的block合并为batch,创建一个rdd
// 一个batch多少个block就有多少个partition
// inputStream.repartition(num):重分区，增加每个batch rdd的partition数量
// 使用kryo序列化，提高序列化task发送到executor上执行的性能，
// 默认输入数据的存储级别是StorageLevel.MEMEORY_AND_DISK_SER_2，recevicer接受数据，默认进行持久化操作，首先序列化，存储到内存
// 内存资源不够，就写入磁盘，而且还会写一份冗余副本到其他executor的block manager，进行数据冗余
// 在spark ui上观察运行情况，如果看到batch处理时间大约batch interval时间，就必须调节batch interval，
// 尽量不要让batch处理时间大于batch interval, 否则batch在内存中积累没法及时计算完成，释放内存空间

public class AdClickRealTimeStatSpark {
	
	private Logger log = LoggerFactory.getLogger(UserVisitSessionAnalyzeSpark6.class.getSimpleName());
	
	public static void main(String[] args) throws InterruptedException {
		// conf
		//Logger log = LoggerFactory.getLogger(AdClickRealTimeStatSpark.class.getSimpleName());
		
		SparkConf conf = new SparkConf()
			.setMaster("local[2]")
			.setAppName("AdClickRealTimeStatSpark")
			.set("spark.serialize", "org.apache.spark.serializer.KryoSerializer") // Kryo序列化，更小空间，自定义类注册
			.set("spark.streamning.blockInterval", "100") // 默认200毫秒,增加block数量，就增加了partition数量，就增加task并行度
			//.set("spark.default.parallelism", "20") // 调节并行度
			.set("spark.streaming.receiver.writeAheadLog", "true"); //HA: 日志预写入
		
		// context:每隔5秒手机最近5秒内的数据源接受过来的数据
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5)); // batch interval时间5秒
		
		// spark streaming 高可用性
		//updateStateByKey and window等状态操作的保存在容错目录
		//jssc.checkpoint("hdfs://localhost:9000/spark/streaming/checkpoint");
		jssc.checkpoint("e:/output/spark/streaming/checkpoint"); 

		
		// kafka参数map,kafaka集群地址host1:port,host2:port,host3:port
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
		
		// kafka topics
		Set<String> topics = new HashSet<>();
		String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
		String[] kafkaTopicsSplitted = kafkaTopics.split(","); //支持多个topics
		for(String topic: kafkaTopicsSplitted){
			topics.add(topic);
		}
		
		// 原始日志：(kafka (timestamp,province,city,userid,adid))
		//创建针对kafka数据来源的输入DStream(discrete stream, 代表了一个源源不断的数据来源，抽象)
		//选用kafka direct API,1.6.3版
		//输入流
		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
				jssc, 
				String.class, 
				String.class, 
				StringDecoder.class, 
				StringDecoder.class, 
				kafkaParams, 
				topics); // k/v的类型和解码
		
		//InputDStream重分区提高并行度
		//adRealTimeLogDStream.repartition(100);
		
		// 实时日志(timestamp,province,city,userid,adid)
		//原始输入流一开始就按照读取mysql中黑名单过滤，两个互相参考
		JavaPairDStream<String,String> filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream); //Tuple2<String,String>
		
		//原始流过滤后生成动态黑名单写入mysql黑名单表中
		generateDynamicBlacklist(filteredAdRealTimeLogDStream);
		
		// 业务功能一：广告实时流量计算，使用updateStateByKey算子全局更新， 并且更新存储到mysql，
		// 每次就是最新(yyyyMMdd_province_city_adid,clickCount)
		JavaPairDStream<String, Long> AdRealTimeStatDStream = calculateRealTimeAdStat(filteredAdRealTimeLogDStream);
		
		//业务功能二：每天每个省份top3热门广告(Row_number() over (partition by order by clickCount) where row_number <=3) or rank

		//calculateProvinceTopAd(AdRealTimeStatDStream);
		calculateProvinceTopAd(conf, AdRealTimeStatDStream); // 使用SparkSession取代SQLContext
		
		//业务功能三：实时统计每天每个广告在最近一个小时滑动窗口内的点击趋势(m每分钟的点击量)
		caculateAdClickCountByWindow(adRealTimeLogDStream); //输入原始DStream
		
		// spark stream实时计算的HA高可用性方案
		// updateStateByKey,window等有状态对的操作，自动进行checkpoint,必须设置checkpoint目录
		
		// spark streaming通过receiver来进行数据接收，接收道德数据，会被划分成一个一个的block,block会被组合成一个batch
		// 针对一个batch,会创建一个rdd, 启动Job来执行我们定义的算子操作
		// receiver接收到数据后立即写入容错的文件系统上的checkpoint目录，作为数据冗余副本, 
		// WAL(Write-Ahead Log) 预写日志机制 
		// spark.streaming.receiver.writeAheadLog enable
		
		/*
		//begin-----新版0.10broker以上
		// https://www.baeldung.com/kafka-spark-data-pipeline
		Map<String, Object> kafkaParams_new = new HashMap<String, Object>();
		kafkaParams_new.put("bootstrap.server", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
		//kafkaParams_new.put("key.deserializer", StringDeserializer.class);
		kafkaParams_new.put("key.deserializer", ConfigurationManager.getObject(Constants.KAFKA_STRING_DE));
		kafkaParams_new.put("value.deserializer", StringDeserializer.class);
		
		//Collection<String> topics_new = Arrays.asList(kafkaTopicsSplitted);
		// https://www.programcreek.com/java-api-examples/index.php?api=org.apache.spark.streaming.kafka010.KafkaUtils
		JavaInputDStream<ConsumerRecord<String, String>> adRealTimeLogDStream_New = org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream(
				jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String,String>Subscribe(topics, kafkaParams_new)); // k/v的类型和解码
		//end-----新版
		
		
		//begin-----新版
		JavaPairDStream<String, Long> dailyUserAdDStream_new = adRealTimeLogDStream_New.mapToPair(new PairFunction<ConsumerRecord<String,String>, String, Long>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Long> call(ConsumerRecord<String, String> t)throws Exception {
				String log = t.value(); // 用key还是value
				String[] logSplitted = log.split(" ");
				String timestamp = logSplitted[0]; //从epch time的毫秒数
				Date date = new Date(Long.valueOf(timestamp)); 
				String datekey = DateUtils.formatDateKey(date);
				long userid = Long.valueOf(logSplitted[3]);
				long adid = Long.valueOf(logSplitted[4]);
				
				//拼接key为一个字符串
				String key = datekey + "_" + userid + "_" + adid; //(yyyyMMdd_user_ad,clickCount
				return new Tuple2<String, Long>(key, 1L);
			}
		});
		//end-----新版
		*/
		

		// 如下spark driver的高可用方案，可以自动被重启
		// 构建完spark streaming上下文之后，记得要进行上下文的启动、等待执行结束、关闭
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
		
	}
	
	/*
		//Driver高可用性：持续不断的更新实时计算程序的元数据（比如有些dstream或者job执行到了那个步骤）
		// 如果后面意外导致driver节点关了，那么可以让spark集成帮助我们自动重启dirver，然后继续运行时候计算程序
		// 并且是接着之前的作业继续执行，没有中断和数据丢失
		// 
		// 在创建和启动streaming context的时候，将元数据写入容错的文件系统，比如hdfs,
		// 在spark-submit脚本中加一些参数，保证在driver关掉后，spark集成可以自己将driver重启启动起来，
		// 而且driver在启动时，不会重新创建一个streaming context,而是从容错文件系统中读取之前元数据
		// 包括job的执行进度
		// 使用这个机制，必须使用cluster模式，确保driver运行在某个worker上面，但是这个模式不方便调试程序
		// 关闭spark streaming 上下文
	// driver HA
	// 1.6旧版才有JavaStreamingContextFactory
	private static void testDriverHA(){
		final String checkpointDirectory = "hdfs://localhost:9000/spark/streaming/checkpoint";
		
		// 工厂启动时先去HA目录查看，如果没有就是第一次
		JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory(){
			@Override
			public JavaStreamingContext create(){
				SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeStatSpark");
				
				// context:每隔5秒手机最近5秒内的数据源接受过来的数据
				JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
				
				// spark streaming 高可用性
				//updateStateByKey and window等状态操作的保存在容错目录
				//jssc.checkpoint("hdfs://localhost:9000/spark/streaming/checkpoint");
				jssc.checkpoint("e:/output/spark/streaming/checkpoint"); 
				// kafka参数map,kafaka集群地址host1:port,host2:port,host3:port
				Map<String, String> kafkaParams = new HashMap<String, String>();
				kafkaParams.put("metadata.broker.list", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
				
				// kafka topics
				Set<String> topics = new HashSet<>();
				String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
				String[] kafkaTopicsSplitted = kafkaTopics.split(","); //支持多个topics
				for(String topic: kafkaTopicsSplitted){
					topics.add(topic);
				}
				
				// 原始日志：(kafka (timestamp,province,city,userid,adid))
				//创建针对kafka数据来源的输入DStream(discrete stream, 代表了一个源源不断的数据来源，抽象)
				//选用kafka direct API,1.6.3版
				//输入流
				JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
						jssc, 
						String.class, 
						String.class, 
						StringDecoder.class, 
						StringDecoder.class, 
						kafkaParams, 
						topics); // k/v的类型和解码
				
				// 业务功能
				//原始输入流一开始就按照读取mysql中黑名单过滤，两个互相参考
				JavaPairDStream<String,String> filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream); //Tuple2<String,String>
				
				//原始流过滤后生成动态黑名单写入mysql黑名单表中
				generateDynamicBlacklist(filteredAdRealTimeLogDStream);
				
				// 业务功能一：广告实时流量计算，使用updateStateByKey算子全局更新， 并且更新存储到mysql，
				// 每次就是最新(yyyyMMdd_province_city_adid,clickCount)
				JavaPairDStream<String, Long> AdRealTimeStatDStream = calculateRealTimeAdStat(filteredAdRealTimeLogDStream);
				
				//业务功能二：每天每个省份top3热门广告(Row_number() over (partition by order by clickCount) where row_number <=3) or rank
				// 什么时候统计
				
				//业务功能三：实时统计每天每个广告在最近一个小时滑动窗口内的点击趋势(m每分钟的点击量)
				caculateAdClickCountByWindow(adRealTimeLogDStream); //输入原始DStream
				
				return jssc;
			}
		};
		
		JavaStreamingContext context = JavaStreamingContext.getOrCreate(checkpointDirectory, contextFactory); //从HA可以恢复
		context.start();
		context.awaitTermination();
		context.close();
		
		// 这种方式运行要在spark-submit中设置以下两个参数, 实现driver高可用
		// --deploy-mode cluster, 默认是client，确保driver是在集群中某个worker上面运行
		// --supervise， 负责时间监控driver
	}
	*/
	
	
	
	
	
	//刚刚接收到原始日志的用户点击行为，根据mysql中动态黑名单过滤，进行实时黑名单过滤，
	// 使用transform算子
	/**
	 * 
	 * @param adRealTimeLogDStream
	 * @return
	 */
	private static JavaPairDStream<String,String> filterByBlacklist(JavaPairInputDStream<String, String> adRealTimeLogDStream){
		
		// 在刚刚接手到原始的用户点击行为日志之后，根据mysql中的动态黑名单，进行实时黑名单过滤
		// 使用transform算子(将dstream中的每个batch 中基于rdd进行处理，装换为任意其他RDD，功能强大)
		// 实时日志(timestamp,province,city,userid,adid)
		JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair( // 使用transform算子
				new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
				//从mysql中查询所有黑名单用户，转换成rdd
				IAdBlacklistDAO dao = DAOFactory.getAdBlacklistDAO();
				List<AdBlacklist> adBlacklists = dao.findAllBlacklist(); //从mysql拿到黑名单，list怎么转换成pairRDD
				
				List<Tuple2<Long, Boolean>> tuples = new ArrayList<>();// 黑名单列表
				
				for(AdBlacklist adBlacklist:adBlacklists){ //循环黑名单
					tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));// 每个黑名单封装到tuple里面
				}
				JavaSparkContext sc = new JavaSparkContext(rdd.context());//从rdd拿到它的context
				JavaPairRDD<Long, Boolean> blackListRDD = sc.parallelizePairs(tuples); //并行化
				
				// 原始RDD tuple-> (user,tuple)
				JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String,String>, Long, Tuple2<String, String>>() {
					private static final long serialVersionUID = 1L;
					public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
						
						String log = t._2; // kafka中传过来的第一个栏位没有意义，每个具体含义
						String[] logSplitted = log.split(" "); //1575700483925 California Los Angeles 20 5
						long userid = Long.valueOf(logSplitted[3]);  //原始日志：(timestamp,province,city,userid,adid)
						return new Tuple2<Long, Tuple2<String, String>>(userid, t);
					}
				});
				
				// 将原始数据RDD和黑名单RDD，left join,如果join后true,则表示这个点击需要过滤
				// Optional可能有，可能没有
				JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> JoinedRDD = 
						 mappedRDD.leftOuterJoin(blackListRDD);
				
				// JoinedRDD过滤黑名单
				JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = JoinedRDD.filter(t ->{
					Optional<Boolean> optional = t._2._2();
					if(optional.isPresent() && optional.get()){ //存在值并且里面包含的是true
						return false;
					}
					return true;
				});
				
				//join并过滤后的结果转换成原始的数据类型
				JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(t ->t._2._1); // lambda
								
				return resultRDD;
			}
		});
		return filteredAdRealTimeLogDStream;
		
	}
	
	
	
	// 生成动态黑名单
	/**
	 * 
	 * @param filteredAdRealTimeLogDStream
	 */
	private static void generateDynamicBlacklist(JavaPairDStream<String, String> filteredAdRealTimeLogDStream){
		
		
		// 一条已提交实时日志(timestamp,province,city,userid,adid)
		// 每5秒的batch数据中，每天每个用户的点击
		//JavaPairDStream<String, Long> dailyUserAdDStream= adRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
		JavaPairDStream<String, Long> dailyUserAdDStream= filteredAdRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Long> call(Tuple2<String, String> t)throws Exception {
				String log = t._2; // kafka中传过来的第一个栏位没有意义，每个具体含义
				String[] logSplitted = log.split(" ");
				String timestamp = logSplitted[0]; //从epoch time的毫秒数
				Date date = new Date(Long.valueOf(timestamp)); 
				String datekey = DateUtils.formatDateKey(date);
				long userid = Long.valueOf(logSplitted[3]);
				long adid = Long.valueOf(logSplitted[4]);
				
				//拼接key为一个字符串
				String key = datekey + "_" + userid + "_" + adid; 
				return new Tuple2<String, Long>(key, 1L); // (yyyyMMdd_user_ad,1L)
			}
		});
		
		//源源不断，每个5s中，每天每个用户的点击统计(yyyyMMdd_user_ad,clickCount)
		JavaPairDStream<String, Long> dailyUserAdCountDStream= dailyUserAdDStream.reduceByKey((v1,v2) -> v1+v2); // lambda函数，这个reduce不是batch内的吗？
		//JavaPairDStream<String, Long> dailyUserAdCountDStream2= dailyUserAdDStream.reduceByKey((Long v1,Long v2) -> v1+v2); // lambda函数，这个reduce不是batch内的吗？

		
		// 数据(yyyyMMdd_user_ad,clickCount)写入mysql， 可以累积一天每个用户对每个广告的点击量吗
		// 实时计算结果插入mysql,两种模式
		// 第一种：day_user_ad插入前先select，没有就insert,有就更新，每个可以对应一个记录
		// 第二种：DB中维护一个timestamp,同一个key,5s一个batch，每间隔5秒就有一个记录插入进去，相当于维护了一个key的多版本，
		// 类似Hbase,而且它不区分update和insert，统一维护一个rowkey
		//dailyUserAdCountDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
		dailyUserAdCountDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
			private static final long serialVersionUID = 1L;
			@Override
			//public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
			public void call(JavaPairRDD<String, Long> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() { //无返回值的函数
					private static final long serialVersionUID = 1L;
					@Override
					public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
					// 分区的数据获取一次数据库连接
						List<AdUserClickCount> adUserClickCountList = new ArrayList<AdUserClickCount>();
						while(iterator.hasNext()){
							Tuple2<String, Long> tuple = iterator.next();
							String[] keys = tuple._1.split("_");
							long clickCount = tuple._2;
							//注意：字符串类型到日期类型再到字符串类型，yyyy--MM--dd
							//String date = DateUtils.formatDateKey(DateUtils.parseDateKey(keys[0])); //从字符串类型到date类型
							String date = DateUtils.formatDate(DateUtils.parseDateKey(keys[0])); //yyyy-MM-dd
							long userid = Long.valueOf(keys[1]);
							long adid =   Long.valueOf(keys[2]);
							
							AdUserClickCount adUserClickCount = new AdUserClickCount();
							adUserClickCount.setDate(date);
							adUserClickCount.setUserid(userid);
							adUserClickCount.setAdid(adid);
							adUserClickCount.setClickCount(clickCount);
							
							adUserClickCountList.add(adUserClickCount);
						}
						
						if(adUserClickCountList.size() >0){
							IAdUserClickCountDAO dao = DAOFactory.getAdUserClickCountDAO();
							// 按照每个partition批量写入db, 这里是每batch的点击，
							// 方法一：如果在spark中使用updateStateByKey维护yyyyMMdd_user_ad全局累加，就不用查db了
							// 方法二：如果在db中记录所有点击，使用sql的sum(*)统计yyyyMMdd_user_ad次数
							//dao.updateBatch(adUserClickCountList); //相同键就更新
							dao.insertBatch(adUserClickCountList); //只管插入DB
							
						}

					}
				});
				//return null;
			}

		});
		
		
		// mysql里面，已经有了累计的每天各个用户对各广告的点击量，遍历每个batch中所有记录，每条记录都要查询一下
		// 这一天这个用户对这个广告的累计点击量多少，如果是100，就判定这个用户就是黑名单用户，写入MySQL表中持久化
		// 前面每个batch已经更新到了mysql，现在每个batch每个记录在此查询
		// 现在过滤每个记录
		JavaPairDStream<String, Long> blacklistDStream = dailyUserAdCountDStream.filter(new Function<Tuple2<String,Long>, Boolean>() {
			private static final long serialVersionUID = 1L;
			public Boolean call(Tuple2<String, Long> tuple) throws Exception {
				String key = tuple._1;
				String[] keySplited = key.split("_");
				String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0])); //从字符串类型到date类型
				long userid = Long.valueOf(keySplited[1]);
				long adid =   Long.valueOf(keySplited[2]);
				
				//要么spark聚合，要么db聚合
				IAdUserClickCountDAO dao = DAOFactory.getAdUserClickCountDAO();
				//int clickCount = dao.findClickCountByKeyDAO(date, userid, adid);
				int clickCount = dao.findClickCountByKeyDAO_SUM(date, userid, adid); //sun
				if(clickCount >= 100){ // 一天一个用户对一只广告点击超过10次就是恶意刷广告行为；
					return true; //黑名单用户
				}
				return false;
			}
		});
		
		// blacklistDStream
		// 里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
		// 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
		// 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
		// 我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
		// 所以直接插入mysql即可
		
		// 在插入前要进行去重,比如
		// <yyyyMMdd_userid_adid count>
		// 20151220_10001_10002 100
		// 20151220_10001_10003 100
		// 10001这个userid就重复了
		
		// <yyyyMMdd_userid_adid count>中拿到userid
		JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map((Tuple2<String,Long> t) -> Long.valueOf(t._1.split("_")[1])); 
		
		//userid全局去重,transform算子对DStream中每个rdd
		JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform((JavaRDD<Long> rdd) -> rdd.distinct());
		
		//userid高效写入mysql, DStream流中每个RDD中每个partition使用连接池
		//distinctBlacklistUseridDStream.foreachRDD(new Function<JavaRDD<Long>,Void>(){ // 每个RDD
		distinctBlacklistUseridDStream.foreachRDD(new VoidFunction<JavaRDD<Long>>(){ // 每个RDD
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<Long> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Long>>() { //每个partition

					private static final long serialVersionUID = 1L;
					@Override
					public void call(Iterator<Long> iter) throws Exception {
						// TODO Auto-generated method stub
						List<AdBlacklist> adBlacklists = new ArrayList<>();
						while(iter.hasNext()){
							long userid = iter.next();
							AdBlacklist user = new AdBlacklist();
							user.setUserid(userid);
							adBlacklists.add(user);
						}
						IAdBlacklistDAO dao = DAOFactory.getAdBlacklistDAO();
						dao.insertBatch(adBlacklists);
					}
				});
				//return null; //void什么都不要return,包括null
			}
		});
		
	} // end:generateDynamicBlacklist
	
	
	
	// 广告实时计算，返回聚合后的数据
	private static JavaPairDStream<String, Long> calculateRealTimeAdStat(JavaPairDStream<String,String> filteredAdRealTimeLogDStream){
		// 计算每天各省各城市各广告的点击量，实时写入更新到mysql中，然后J2EE系统每隔几秒从mysql中读取一次最新数据，每次都可能不一样
				// 维度：日期，省份，城市，广告(date,province,city,adid)
				// 通过spark，直接统计出来全局的点击册数，在spark集群中保留一份，mysql中也保留一份
				// 原始数据map成<date_province_city_adid, 1>格式，执行updateStateByKey算子，这个算子在spark集群内存中，维护一份key的全局状态
				JavaPairDStream<String, Long> adMappedDStream= filteredAdRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Long> call(Tuple2<String, String> t)throws Exception {
						String log = t._2; // kafka中传过来的第一个栏位没有意义，每个具体含义
						String[] logSplitted = log.split(" ");
						String timestamp = logSplitted[0]; //从epoch time的毫秒数
						Date date = new Date(Long.valueOf(timestamp));
						String dateKey= DateUtils.formatDateKey(date); //格式化成yyyyMMdd
						String province = logSplitted[1];
						String city = logSplitted[2];
						//long userid = Long.valueOf(logSplitted[3]);
						long adid = Long.valueOf(logSplitted[4]);
						//拼接key为一个字符串
						String key = dateKey + "_" + province + "_" + city + "_" + adid; //
						return new Tuple2<String, Long>(key, 1L); // (yyyyMMdd_province_city_adid,1L)
					}
				});
				
				// 在DStream中，每个batch rdd累加的各个key,
				// 每次计算最新的值，updateStateByKey在spark集群内存中，维护一份key的全局状态
				//Spark Stream中updateStateByKey感觉像是Spark中的Accumulator
				JavaPairDStream<String, Long> aggragatedDStream= adMappedDStream.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
						// 对每个key都会调用一次这个方法
						// 比如key是<20191205_Hubei_Wuhan_10001,1>,values (1,1,1,1,1)就会调用这个方法5个
						// 首先根据optionl判断，之前这个key，是否有对应的状态
						long clickCount = 0L;
						// 如果之前存在这个状态，那就以之前的状态作为起点，进行值的累计
						if(optional.isPresent()){
							clickCount = optional.get();
						}
						// values代表了在一个batch 的所有rdd中每个key对应的所有的值
						for(Long value:values){
							clickCount += value;
						}

						return Optional.of(clickCount); // (yyyyMMdd_province_city_adid,clickCount)
					}
				}); 
				
				
				//updateStateByKey最新结果插入mysql中
				aggragatedDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Long>>() {
					private static final long serialVersionUID = 1L;
					public void call(JavaPairRDD<String, Long> rdd) throws Exception {
						rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
							private static final long serialVersionUID = 1L;
							public void call(Iterator<Tuple2<String, Long>> iter) throws Exception {
								List<AdStat> adStatList = new ArrayList<>();
								while(iter.hasNext()){
									Tuple2<String, Long> t= iter.next(); // (yyyyMMdd_province_city_adid,clickCount)
									
									String key = t._1; // kafka中传过来的第一个栏位没有意义，每个具体含义
									String[] keySplitted = key.split("_");
									//String timestamp = logSplitted[0]; //从epoch time的毫秒数
									//Date date = new Date(Long.valueOf(timestamp)); 
									//String datekey = DateUtils.formatDateKey(date);
									String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplitted[0])); //yyyy-MM-dd写DB
									String province = keySplitted[1];
									String city = keySplitted[2];
									long adid = Long.valueOf(keySplitted[3]);
									//long clickCount = Long.valueOf(keySplitted[4]); //错误
									long clickCount = t._2;
									
									AdStat adStat = new AdStat(); //封装
									adStat.setDate(date);
									adStat.setProvince(province);
									adStat.setCity(city);
									adStat.setAdid(adid);
									adStat.setClickCount(clickCount);
									
									adStatList.add(adStat);
								}
								
								IAdStatDAO dao = DAOFactory.getAdStatDAO();
								//有就更新，没有就插入
								//dao.insertBatch(adStatList); //一个batch中的一个rdd的一个partition里ad批量插入一次
								// bug: 重复记录
								dao.updateBatch(adStatList);
							
								
							}
						});
					}
				});
				return aggragatedDStream;
	}
	
	/**
	 * 每个省每天热门广告
	 * @param AdRealTimeStatDStream :每天全量的各省份各城市各广告的点击量,<yyyyMMdd_province_city_adid,count>
	 * 使用spark sql window函数，rdd里面的tuple需要转换为Row，
	 * 使用transform将每个rdd中元素转换为Row，然后sql得到新的Row，然后row读取后封装好写入mysql
	 */ 
	//private static void calculateProvinceTopAd(JavaPairDStream<String, Long> AdRealTimeStatDStream){
	private static void calculateProvinceTopAd(SparkConf conf, JavaPairDStream<String, Long> AdRealTimeStatDStream){
		
		JavaDStream<Row> rowsDStream  = AdRealTimeStatDStream.transform(new Function<JavaPairRDD<String,Long>, JavaRDD<Row>>() {

			private static final long serialVersionUID = 1L;

			public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
				//<yyyy-MM-dd_province_city_adid,count> -> <yyyyMMdd_province_adid,count>
				JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String,Long>, String, Long>() {
					private static final long serialVersionUID = 1L;
					public Tuple2<String, Long> call(Tuple2<String, Long> t) throws Exception {
						String[] keySplitted = t._1.split("_");
						String date = keySplitted[0]; //字符串到日期再到字符串，保证时间的正确解析
						//String date = DateUtils.formatDateKey(DateUtils.parseDateKey(keySplitted[0]));
						String province = keySplitted[1];
						long adid = Long.valueOf(keySplitted[3]);
						long clickCount = t._2;
						
						String key = date + "_" + province + "_" + adid;
						return new Tuple2<String, Long>(key, clickCount);
					}
				});
				System.out.println("myTEST:spark ROW--------------");
				System.out.println(mappedRDD.take(1)); // 结果列表第一个元素
				
				// 去掉了city，需要reduceByKey
				JavaPairRDD<String, Long> aggregateRDD = mappedRDD.reduceByKey((v1,v2) -> v1+v2);
				
				// 转换为JavaPairRDD<String, Long> -> JavaRDD<Row>
				// 注册为临时表，spark sql
				JavaRDD<Row> rowsRDD = aggregateRDD.map(new Function<Tuple2<String,Long>, Row>() {
					private static final long serialVersionUID = 1L;

					public Row call(Tuple2<String, Long> t) throws Exception { //<yyyyMMdd_province_adid,count>
						String[] keySplitted = t._1.split("_");
						String dateKey = keySplitted[0]; //yyyyMMdd
						String date = DateUtils.formatDate(DateUtils.parseDateKey(dateKey)); // 格式化日期yyyy-MM-dd
						String province = keySplitted[1];
						long adid = Long.valueOf(keySplitted[2]);
						long clickCount = t._2;
						return RowFactory.create(date,province,adid,clickCount); // 最终返回了JavaRDD<Row>类型
					}
				});

				
				//spark sql schema
				StructType schema = DataTypes.createStructType(Arrays.asList(
						DataTypes.createStructField("date", DataTypes.StringType, true),
						DataTypes.createStructField("province", DataTypes.StringType, true),
						DataTypes.createStructField("ad_id", DataTypes.LongType, true),
						DataTypes.createStructField("click_count", DataTypes.LongType, true)));
						
				
				//HiveContext sqlContext = new HiveContext(rdd.context()); //类加载出错，都过时了
				//SQLContext sqlContext = new SQLContext(rdd.context()); // 都过时了
				//Dataset<Row> dailyAdClickCountDF_old = sqlContext.createDataFrame(rowsRDD, schema); // 从sqlContext中创建dataframe
				
				// spark session 替代过时的SQLContext,HiveContext
				SparkSession spark = SparkSession.builder().appName("calculateProvinceTopAd").config(conf).getOrCreate();
				Dataset<Row> dailyAdClickCountDF = spark.createDataFrame(rowsRDD, schema);
				
				//Dataset<Row> df = sparkSession.createDataFrame(rowsRDD, schema); // SparkSession
				//df.registerTempTable("user_visit_action"); // 过时了
				//df.createGlobalTempView("tmp_daily_ad_click_count_by_prov"); //
				dailyAdClickCountDF.createOrReplaceGlobalTempView("tmp_daily_ad_click_count_by_prov"); //可能创建不成功,表访问格式global_temp.table
				
				
				// 使用spark sql执行SQL语句，配合window function,统计各省份top3
				// spark窗口函数 https://knockdata.github.io/spark-window-function/
				// https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-functions-windows.html
				//Dataset<Row> provinceTopAdDF = sqlContext.sql(
				Dataset<Row> provinceTopAdDF = spark.sql(
						"SELECT date,province,ad_id,click_count FROM (" //外层查询
							+ "SELECT date,province,ad_id,click_count,"
								//+ "ROW_NUMBER() OVER(PARTITION BY date,province ORDER BY click_count DESC) AS rank " //row_number() over(partition by col order by col desc) colName
								+ "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) AS rank " // 多个栏位partition，
								+ "FROM global_temp.tmp_daily_ad_click_count_by_prov"
							+ ") tmp "
							+ "WHERE rank <=3"); 
				
				/*
				// 在sqlContext中解决global_temp查询, 外查询和内查询分开
				//Dataset<Row> provinceTopAdDF_sub = sqlContext.sql(
				Dataset<Row> provinceTopAdDF_sub = spark.sql(
						//"SELECT date,province,ad_id,click_count FROM (" //内层查询
							"SELECT date,province,ad_id,click_count," //内层查询
								//+ "ROW_NUMBER() OVER(PARTITION BY date,province ORDERY BY click_count DESC) AS rank" //row_number() over(partition by col order by col desc) colName
								+ "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) AS rank " // 多个栏位partition，
								+ "FROM global_temp.tmp_daily_ad_click_count_by_prov"
								);
							//+ ") tmp " 
							//+ "WHERE rank <=3");
				
				provinceTopAdDF_sub.createOrReplaceGlobalTempView("tmp_daily_ad"); //外层查询
				//Dataset<Row> provinceTopAdDF = sqlContext.sql("SELECT "
				Dataset<Row> provinceTopAdDF = spark.sql("SELECT "
						+ "date,province,ad_id,click_count "
						+ "FROM global_temp.tmp_daily_ad "
						+ "WHERE rank <=3");
				*/
				
				//return rowsRDD;
				return provinceTopAdDF.javaRDD();
			}
		});
		
		// rowsDStream批量数据写入mysql中
		rowsDStream.foreachRDD(new VoidFunction<JavaRDD<Row>>() { //DSTream中每一个rdd

			private static final long serialVersionUID = 1L;

			public void call(JavaRDD<Row> rdd) throws Exception {
				
				rdd.foreachPartition(new VoidFunction<Iterator<Row>>() { // rdd中每一个partition

					private static final long serialVersionUID = 1L;
					public void call(Iterator<Row> iter) throws Exception {
						
						List<AdProvinceTop3> adList = new ArrayList<>(); //每个partition中
						while(iter.hasNext()){
							Row row = iter.next();
							String date = row.getString(0);
							String province = row.getString(1);
							long adid = row.getLong(2);
							long clickCount = row.getLong(3);
							
							AdProvinceTop3 ad = new AdProvinceTop3();
							ad.setDate(date);
							ad.setProvince(province);
							ad.setAdid(adid);
							ad.setClickCount(clickCount);
							
							adList.add(ad);
						}
						
						IAdProvinceTop3DAO dao = DAOFactory.getAdProvinceTop3DAO(); //工厂静态方法
						dao.updateBatch(adList);

					}
				});
			}
		});
		
	}
	
	
	/**
	 * 最近一小时滑动窗口内的广告点击趋势,原始日志：(kafka (timestamp,province,city,userid,adid))
	 * 先mapToPair映射原始字符串到分钟级别 (yyyyMMddHHmm_ad,1L)， 再reduceByKeyAndWindow(60分钟时长，10秒刷新)，
	 * 最后 foreachRDD和foreachParition持久化到mysql
	 * @param adRealTimeLogDStream
	 */
	private static void caculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream){
		
		JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
				String log = t._2; // kafka中传过来的第一个栏位没有意义，我们的信息在第二个
				String[] logSplitted = log.split(" ");
				String timestamp = logSplitted[0]; //从epoch time的毫秒数
				Date date = new Date(Long.valueOf(timestamp)); //日期格式
				String datekey = DateUtils.formatTimeMinute(date); //格式化为yyyyMMddHHmm
				//long userid = Long.valueOf(logSplitted[3]);
				long adid = Long.valueOf(logSplitted[4]);
				
				//拼接key为一个字符串,
				String key = datekey + "_"  + adid; 
				return new Tuple2<String, Long>(key, 1L); // (yyyyMMddHHmm_ad,1L)
			}
		});
		
		// 过来的每个batch rdd，都会被映射成(yyyyMMddHHmm_ad,1L)，
		// 同一个分钟内相同广告点击数reduceByKeyAndWindow
		JavaPairDStream<String, Long> aggWindowDStream = pairDStream.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Long call(Long v1, Long v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		}, Durations.minutes(60), Durations.seconds(10)); //滑动窗口：长度60分钟，每10秒滑动更新一次
		
		//结果持久化到Mysql
		aggWindowDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Long>>() {

			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaPairRDD<String, Long> rdd) throws Exception {
				// TODO Auto-generated method stub
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {

					private static final long serialVersionUID = 1L;
					@Override
					public void call(Iterator<Tuple2<String, Long>> iter) throws Exception {
						List<AdClickTrend> adList = new ArrayList<>();
						while(iter.hasNext()){
							Tuple2<String, Long> t = iter.next();
							String[] keysplitted = t._1.split("_"); //  (yyyyMMddHHmm_ad,count)
							String dateMinute = keysplitted[0];
							long adid = Long.valueOf(keysplitted[1]);
							long clickCount = t._2;
							//String[] keySplitted = t._1.split("_"); // (yyyyMMddHHmm_ad,count)
							// 日期在写入DB前格式转换成yyyy-MM-dd
							String date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)));
							String hour = dateMinute.substring(8, 10);
							String minute = dateMinute.substring(10, 12);
							//long adid = Long.valueOf(keys.substring(13, 15));

							AdClickTrend ad = new AdClickTrend();
							ad.setDate(date);
							ad.setHour(hour);
							ad.setMinute(minute);
							ad.setAdid(adid);
							ad.setClickCount(clickCount);
							
							adList.add(ad);
						}
						
						IAdClickTrendDAO dao = DAOFactory.getAdClickTrendDAO(); //DAO工厂方法
						dao.updateBatch(adList); // 批量更新到mySQL中
						

					}
				});
			}
		});
	}
	
}
