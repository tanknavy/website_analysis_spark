package com.github.tanknavy.kafka.aggregator;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.maven.artifact.transform.LatestArtifactTransformation;
import org.joda.time.DateTime;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.tanknavy.avro.CourseStatistic;
import com.github.tanknavy.avro.Review;
import com.typesafe.config.ConfigFactory;

/**
 * "name": "CourseStatistic",
  "fields": [
    {"name": "course_id", "type": "long", "default": -1, "doc": "Course ID in Udemy's DB"},
    {"name": "course_title", "type": "string", "default": "", "doc": "Course Title"},
    {"name": "average_rating",  "type": "double", "default": 0},
    {"name": "count_reviews",  "type": "long", "default": 0},
    {"name": "count_five_stars",  "type": "long", "default": 0},
    {"name": "count_four_stars",  "type": "long", "default": 0},
    {"name": "count_three_stars",  "type": "long", "default": 0},
    {"name": "count_two_stars",  "type": "long", "default": 0},
    {"name": "count_one_star",  "type": "long", "default": 0},
    {"name": "count_zero_star",  "type": "long", "default": 0},
    {"name": "last_review_time",  "type": {"type": "long", "logicalType": "timestamp-millis"}, "doc": "last review in aggregation"},
    {"name": "sum_rating",  "type": "double", "default": 0}
 * @author admin
 * https://github.com/simplesteph/medium-blog-kafka-udemy
 *
 */

public class AggregatorMain {

	private Logger log = LoggerFactory.getLogger(AggregatorMain.class
			.getSimpleName());
	private AppConfig appConfig;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AggregatorMain aggregatorMain = new AggregatorMain();
		aggregatorMain.start();
	}

	private AggregatorMain() { // 构造方法装载配置
		appConfig = new AppConfig(ConfigFactory.load());
	}

	private void start() {
		// TODO Auto-generated method stub
		Properties config = getKafKaStreamsConfig();
		KafkaStreams streams = createTopology(config);
		streams.cleanUp();
		streams.start();
		// http://tutorials.jenkov.com/java/lambda-expressions.html
		// Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		// //功能同下？
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			streams.close();
		}));
	}

	private Properties getKafKaStreamsConfig() {
		// TODO Auto-generated method stub
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,appConfig.getBootstrapServers()); // kafka机器
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class); //key默认序列化器（String）
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class); // value默认序列化类(定制)
		config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()); // 序列化器注册位置

		return config;
	}

	private KafkaStreams createTopology(Properties config) {
		// 定义Review 和CourseStatistic类的 serdes工具，be used for reading and writing data in "specific Avro" format
		SpecificAvroSerde<Review> reviewSpecificAvroSerde = new SpecificAvroSerde<>(); // 定制序avro列化器
		SpecificAvroSerde<CourseStatistic> courseStatisticSpecificAvroSerde = new SpecificAvroSerde<>(); // 定制序avro列化器

		reviewSpecificAvroSerde.configure(Collections.singletonMap( // 返回可序列化可变的map 键值对
				KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false); // Avro配置，key不需要avro序列化
		courseStatisticSpecificAvroSerde.configure(Collections.singletonMap(
				KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false); // Avro配置，key不需要avro序列化

		Serdes.LongSerde longSerde = new Serdes.LongSerde(); // 长整型序列化
		Serdes.StringSerde stringSerde = new Serdes.StringSerde(); // 字符串序列化

		StreamsBuilder builder = new StreamsBuilder(); // 可以指定kafaka stream拓扑（source->process-sink）高级api

		// 创建stream使用timestamp extractor
		// KStream is an abstraction of a record stream of KeyValue pairs,each record is an independent entity/event in the real world
		KStream<String, Review> validReviews = builder.stream( //KStream抽象化的键值对的record stream， builder.stream从指定topic
				appConfig.getValidTopicName(), // 主题
				Consumed.with(longSerde, reviewSpecificAvroSerde, // Consumed可以创建KStream,KTable,GlobalTable, with提供的参数（键值序列化器..）
						new ReviewTimestampExtractor(),// 按照时间抽取
						null)) // null: offset reset policy
						.selectKey((Key, review) -> review.getCourse().getId().toString()); // 每条记录设置新的key,stateless操作, 这里按照课程Id分组

		// 从上游得到的流中，创建长期topology
		// KTable is an abstraction of a changelog stream from a primary-keyed table
		KTable<String, CourseStatistic> longTermCourseStats = validReviews
				.groupByKey() // 上游的sellectKey这里就按照课程分组
				.<CourseStatistic>aggregate( //聚集记录值，前面的<>泛型表示aggregate要处理的对象类型
						this::emptyStats, //initializer，这里是设置每个CourseStatistic的LastReviewTime时间为0, 为了在时间窗口确定最小时间戳
						this::reviewAggregator, // aggregator，这里输入参数（courseId, Review, CourseStatistic), 返回CourseStatistic， 从每个reivew中courseTitle和courseId马上可知
						Materialized // 物化StateStore ->(k,s,v)，v是type of state store (note: state stores always have key/value types <Bytes,byte[]>
								.<String, CourseStatistic, KeyValueStore<Bytes, byte[]>> as(
										"long-term-stats").withValueSerde(courseStatisticSpecificAvroSerde)); //指定物化时值的序列化方法
		//changelog stream to a KStream到 新的topic
		longTermCourseStats.toStream().to(appConfig.getLongTermStatsStatsTopicName(),
				Produced.with(stringSerde, courseStatisticSpecificAvroSerde)); //序列化器
		
		
		// 90天的average
		// 91天的时间窗口，向前1天的滑动
		Duration windowSizeDuration = Duration.ofDays(91);
		Duration advanceDuration = Duration.ofDays(1);
		long windowSizeMs = windowSizeDuration.toMillis();
		long advanceMs = advanceDuration.toMillis();
		// kafka时间窗口
		TimeWindows timeWindow = TimeWindows.of(windowSizeDuration).advanceBy(advanceDuration);
		
		//KTable is an abstraction of a changelog stream from a primary-keyed table
		KTable<Windowed<String>, CourseStatistic> windowedCourseStatisticKTable = validReviews
				.filter((k,review) -> !isReviewExpired(review, windowSizeMs)) // 过滤只要符合条件的
				.groupByKey() // 上游的sellectKey这里就按照课程分组
				.windowedBy(timeWindow) // 执行windowed聚集,  a windowed KTable has type <Windowed<K>,V>.    
				.<CourseStatistic>aggregate( //initializer, aggregator, materialized
						this::emptyStats, //initializer，这里是初始化CourseStatistic的LastReviewTime时间
						this::reviewAggregator, // aggregator，这里（courseId, Review, CourseStatistic）
						Materialized.<String, CourseStatistic, WindowStore<Bytes, byte[]>>as("recent-stats") // materialized
						.withValueSerde(courseStatisticSpecificAvroSerde));
		
		// 上述Ktable转换到KStream
		KStream<String, CourseStatistic> recentStats = windowedCourseStatisticKTable
				.toStream() //changelog stream to a KStream到 新的topic
				.filter((window,couseStat) -> keepCurrentWindow(window,advanceMs))
				.peek((key,value) -> log.info(value.toString()))
				.selectKey((k,v) ->k.key()); // <Windowed<K>,V>，Windowed键转换后取得原始键,这是是courseId
		//KStream写入 短期topic
		recentStats.to(appConfig.getRecentStatsTopicName(), Produced.with(stringSerde, courseStatisticSpecificAvroSerde));
		
		/*
		//方法二----------------------------
		// 不适用Windowed时间窗口，使用较低的api手动编写创造 state store
		StoreBuilder<KeyValueStore<Long, Review>> recentReviewStore = 
				Stores.keyValueStoreBuilder( //（supplier, keySerde, valueSerde），A store supplier that can be used to create one or more KeyValueStore instances of type <Byte, byte[]>.
						Stores.persistentKeyValueStore("persistent-counts"),
						Serdes.Long(),
						reviewSpecificAvroSerde);
		// 添加store到topology,Adds a state store to the underlying Topology
		builder.addStateStore(recentReviewStore);
		Long timeToKeepAReview = TimeUnit.DAYS.toMillis(90);
		// 转换输入流中每条记录从输入流到输出流
		KStream<String, Review> recentReviews = validReviews.transform( //A Transformer (provided by the given TransformerSupplier) is applied to each input record 
				new RecentReviewsTransformerSupplier(timeToKeepAReview,recentReviewStore.name()),
				recentReviewStore.name());
		
		// 创建recent stats topology
		KTable<String, CourseStatistic> recentCourseStats = recentReviews
				.groupByKey()
				.<CourseStatistic>aggregate( //initializer, aggregator)
				this::emptyStats,
				this::reviewAggregator,
				Materialized.<String, CourseStatistic, KeyValueStore<Bytes, byte[]>>as("recent-stats-alt")
				.withValueSerde(courseStatisticSpecificAvroSerde));
		
		recentCourseStats.toStream()
				.peek((key,value) -> log.info(value.toString()))
				.to(appConfig.getRecentStatsTopicName() + "-low-api", Produced.with(stringSerde,courseStatisticSpecificAvroSerde));
		//----------------------------------
		*/
		
		return new KafkaStreams(builder.build(),config);
	}
	
	
	

	private Boolean keepCurrentWindow(Windowed<String> window, long advanceMs) { // windowed：聚集的键类型
		// TODO Auto-generated method stub
		long now = System.currentTimeMillis();
		return window.window().end() > now && window.window().end() < now + advanceMs; // end返回end timestamp of this window
	}

	private Boolean isReviewExpired(Review review, Long maxTime) {
		return review.getCreated().getMillis() + maxTime < System
				.currentTimeMillis();
	}

	private CourseStatistic emptyStats() {
		return CourseStatistic.newBuilder().setLastReviewTime(new DateTime(0L)).build(); // 设置上次时间为0
	}
	
	private CourseStatistic reviewAggregator(String courseId, Review newReview, CourseStatistic currentStats) {
		CourseStatistic.Builder courseStatisticBuilder = CourseStatistic.newBuilder(currentStats); // A new CourseStatistic RecordBuilder使用CourseStatistic
		
		courseStatisticBuilder.setCourseId(newReview.getCourse().getId());
		courseStatisticBuilder.setCourseTitle(newReview.getCourse().getTitle());
		
		String reviewRating = newReview.getRating().toString();
		Integer incOrDec = (reviewRating.contains("-")) ? -1:1;
		
		switch (reviewRating.replace("-", "")){
		case "0.5": 
			courseStatisticBuilder.setCountZeroStar(courseStatisticBuilder.getCountZeroStar() + incOrDec);
			break;
		case "1.0":
		case "1.5":
	          courseStatisticBuilder.setCountOneStar(courseStatisticBuilder.getCountOneStar() + incOrDec);
            break;
        case "2.0":
        case "2.5":
              courseStatisticBuilder.setCountTwoStars(courseStatisticBuilder.getCountTwoStars() + incOrDec);
            break;
        case "3.0":
        case "3.5":
              courseStatisticBuilder.setCountThreeStars(courseStatisticBuilder.getCountThreeStars() + incOrDec);
            break;
        case "4.0":
        case "4.5":
              courseStatisticBuilder.setCountFourStars(courseStatisticBuilder.getCountFourStars() + incOrDec);
            break;
        case "5.0":
              courseStatisticBuilder.setCountFiveStars(courseStatisticBuilder.getCountFiveStars() + incOrDec);
            break;
		        
		
		}
        Long newCount = courseStatisticBuilder.getCountReviews() + incOrDec; //数量+1
        Double newSumRating = courseStatisticBuilder.getSumRating() + new Double(newReview.getRating().toString());
        Double newAverageRating = newSumRating / newCount;
        
        // couseStatistic类中需要最后聚集更新的
        courseStatisticBuilder.setCountReviews(newCount);
        courseStatisticBuilder.setSumRating(newSumRating);
        courseStatisticBuilder.setAverageRating(newAverageRating);
        courseStatisticBuilder.setLastReviewTime(latest(courseStatisticBuilder.getLastReviewTime(), newReview.getCreated()));
        
		
		return courseStatisticBuilder.build();
	}

	private DateTime latest(DateTime a, DateTime b) {
		// TODO Auto-generated method stub
		return a.isAfter(b) ? a:b;
	}
	
	
}
