package com.spark;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author medal
 * @create 2019-11-10 11:33
 **/
public class Demo01 {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("Demo01").setMaster("local[3]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(20));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "cdh.medal.com:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
     //   kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("testTopic");

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

//        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
//        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
//        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
//                .reduceByKey((i1, i2) -> i1 + i2);
//        wordCounts.print();

        JavaDStream<String> lines = messages.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                return consumerRecord.value();
            }
        });



      //  lines.print();
        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {

                JavaRDD<JSONObject> baseDStream = stringJavaRDD.map(new Function<String, JSONObject>() {
                    @Override
                    public JSONObject call(String s) throws Exception {
                        JSONObject ob = JSONObject.parseObject(s);
                        return ob;
                    }
                }).filter(new Function<JSONObject, Boolean>() {
                    @Override
                    public Boolean call(JSONObject ob) throws Exception {
                        return ob.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq");
                    }
                });

                //充值成功订单量
                JavaPairRDD<String, Integer> totalSucc = baseDStream.mapToPair(new PairFunction<JSONObject, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(JSONObject ob) throws Exception {
                        String reqId = ob.getString("requestId");
                        // 获取日期
                        String day = reqId.substring(0, 8);
                        // 取出该条充值是否成功的标志
                        String result = ob.getString("bussinessRst");
                        int flag = result.equals("0000") ? 1 : 0;
                        return new Tuple2<>(day, flag);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) throws Exception {
                        return i1 + i2;
                    }
                });

                // 总金额
                JavaPairRDD<String, Double> totalMoney = baseDStream.mapToPair(new PairFunction<JSONObject, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(JSONObject ob) throws Exception {
                        String reqId = ob.getString("requestId");
                        // 获取日期
                        String day = reqId.substring(0, 8);
                        // 取出该条充值是否成功的标志
                        String result = ob.getString("bussinessRst");
                        // 金额
                        Double fee = result.equals("0000") ? Double.parseDouble(ob.getString("chargefree")) : 0;
                        return new Tuple2<>(day, fee);
                    }
                }).reduceByKey(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double d1, Double d2) throws Exception {
                        return d1 + d2;
                    }
                });

                // 总的订单量
                long total = baseDStream.count();

                // 总时间
                JavaPairRDD<String, Long> totalTime = baseDStream.mapToPair(new PairFunction<JSONObject, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(JSONObject ob) throws Exception {
                        String reqId = ob.getString("requestId");
                        // 获取日期
                        String day = reqId.substring(0, 8);
                        // 取出该条充值是否成功的标志
                        String result = ob.getString("bussinessRst");
                        String endTime = ob.getString("receiveNotifyTime");
                        String starTime = reqId.substring(0, 17);

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                        long cost = result.equals("0000") ? sdf.parse(endTime).getTime() - sdf.parse(starTime).getTime() : 0;

                        return new Tuple2<>(day, cost);
                    }
                }).reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long aLong, Long aLong2) throws Exception {
                        return aLong + aLong2;
                    }
                });

            }
        });
        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
