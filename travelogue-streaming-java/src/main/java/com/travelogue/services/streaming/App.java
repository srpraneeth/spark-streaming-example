package com.travelogue.services.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.travelogue.services.streaming.dto.SimplePost;

/**
 * Hello world!
 *
 */
public class App 
{
    @SuppressWarnings("unused")
	public static void main( String[] args )
    {
    	LogManager.getRootLogger().setLevel(Level.OFF);
    	SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
    	sparkConf.setMaster("local[4]");
    	SparkContext sc = new SparkContext();
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,new Duration(1000));
		Map<String, String>rabbitMqConParams = new HashMap<String, String>();
        rabbitMqConParams.put("host", "localhost");
        rabbitMqConParams.put("queueName", "postsQueue");
        LogManager.getRootLogger().setLevel(Level.OFF);
        ObjectMapper mapper = new ObjectMapper();
		JavaReceiverInputDStream<SimplePost> receiverStream = RabbitMQUtils.createJavaStream(jssc, SimplePost.class, rabbitMqConParams, new Function<byte[], SimplePost>() {
			private static final long serialVersionUID = 1L;
			@Override
			public SimplePost call(byte[] bytes) throws Exception {
//				return new String(bytes);
				return mapper.readValue(bytes, SimplePost.class);
			}
		});
		
		JavaDStream<SimplePost> receiverStreamW = receiverStream.window(new Duration(20000), new Duration(1000));
		JavaPairDStream<String, Integer> receiverStreamWReducedByTags = receiverStreamW.flatMapToPair(new PairFlatMapFunction<SimplePost, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<Tuple2<String, Integer>> call(SimplePost arg0) throws Exception {
				List<Tuple2<String, Integer>> result = new ArrayList<>();
				Arrays.asList(arg0.getTags()).stream().filter( x -> { return !x.isEmpty(); }).forEach(new Consumer<String>() {
					@Override
					public void accept(String t) {
						result.add(new Tuple2<String, Integer>(t, arg0.getId()));
					}
				});
				return result;
			}
		});
		
		JavaPairDStream<String, Iterable<Integer>> receiverStreamWReducedByTagsGroupedByKey = receiverStreamWReducedByTags.groupByKey();
			
		
		/*JavaDStream<SimplePost> receiverStreamW = receiverStream.window(new Duration(20000), new Duration(1000));
		JavaDStream<String> flat = receiverStreamW.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(String arg0) throws Exception {
				return Arrays.asList(arg0.split(" "));
			}
		});
		JavaPairDStream<String, Integer> pairs = flat.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				return new Tuple2<String, Integer>(arg0, 1);
			}
		});
		JavaPairDStream<String, Integer> reduce = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		});
		reduce.foreachRDD(new Function<JavaPairRDD<String,Integer>, Void>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Void call(JavaPairRDD<String, Integer> arg0) throws Exception {
				arg0.foreach(new VoidFunction<Tuple2<String,Integer>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public void call(Tuple2<String, Integer> arg0) throws Exception {
						System.out.println("Key" + arg0._1() + " Val " + arg0._2());
					}
				});
				return null;
			}
		});*/
        jssc.start();
        jssc.awaitTermination();
        LogManager.getRootLogger().setLevel(Level.OFF);
    }
}
