package com.spark;

import com.alibaba.fastjson.JSONObject;
import com.utils.KafkaOffsetUtil;
import com.utils.MysqlPool;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * @author medal
 * @create 2019-11-10 11:33
 **/
public class Demo02 {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args)  {
        try {
        SparkConf sparkConf = new SparkConf().setAppName("Demo02");
        sparkConf.setMaster("local[*]");

        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5000");  // Kafka每个分区每次最多5000条
        sparkConf.set("spark.default.parallelism", "6");
        sparkConf.set("spark.streaming.backpressure.enabled", "true");
        sparkConf.set("spark.streaming.kafka.consumer.poll.ms", "4000");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
// 433264   216632
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "cdh.medal.com:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "medalTestOffset");
   //     kafkaParams.put("auto.offset.reset", "latest");
     //   kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("testTopic");

        // 获取偏移量
            Map<TopicPartition,Long> myOffset = null;
            JavaInputDStream<ConsumerRecord<String, String>> messages = null;

            try {
                myOffset = KafkaOffsetUtil.getMyCurrentOffset();
                messages =
                        KafkaUtils.createDirectStream(
                                jssc,
                                LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.Subscribe(topics, kafkaParams,myOffset)
                        );
            } catch (Exception e) { // 如果手动维护偏移量失败，则自动维护偏移量
               // e.printStackTrace();
                messages =
                        KafkaUtils.createDirectStream(
                                jssc,
                                LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.Subscribe(topics, kafkaParams)
                        );
            }

        final AtomicReference<OffsetRange[]> atomicReference = new AtomicReference<>();

        JavaDStream<ConsumerRecord<String, String>> transform
                = messages.transform(new Function<JavaRDD<ConsumerRecord<String, String>>, JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public JavaRDD<ConsumerRecord<String, String>> call(JavaRDD<ConsumerRecord<String, String>> value) throws Exception {

                OffsetRange[] offsetRanges = ((HasOffsetRanges) value.rdd()).offsetRanges();
                atomicReference.set(offsetRanges);
                return value;
            }
        });

        JavaDStream<String> lines = transform.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> consumerRecord) throws Exception {

                return consumerRecord.value();
            }
        });

      //  lines.print();
        JavaDStream<JSONObject> filter = lines.map(new Function<String, JSONObject>() {

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

        // rdd 转换完成
            insertMysql(filter,atomicReference);

            jssc.start();
            jssc.awaitTermination();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static  void insertMysql(JavaDStream<JSONObject> filter
                    ,AtomicReference<OffsetRange[]> atomicReference) {
        //持久化数据
        filter.persist(StorageLevel.MEMORY_ONLY());

        filter.foreachRDD(new VoidFunction<JavaRDD<JSONObject>>() {

            @Override
            public void call(JavaRDD<JSONObject> stringJavaRDD) throws Exception {
              //  OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                // 获取偏移量
               // OffsetRange[] offsetRanges = ((HasOffsetRanges)stringJavaRDD.rdd()).offsetRanges();

                long num = stringJavaRDD.count();
                if (num == 0) {
                    return;
                }
                // stringJavaRDD.coalesce(6);

                JavaPairRDD<String[], Double[]> baseRDD = stringJavaRDD.mapToPair(new PairFunction<JSONObject, String[], Double[]>() {
                    @Override
                    public Tuple2<String[], Double[]> call(JSONObject ob) throws Exception {
                        // 取出该条充值是否成功的标志
                        String result = ob.getString("bussinessRst");

                        double sucess = result.equals("0000") ? 1 : 0;
                        Double fee = result.equals("0000") ? Double.parseDouble(ob.getString("chargefee")) : 0;

                        // 充值发起时间和结束时间
                        String requestId = ob.getString("requestId");

                        // 获取日期
                        String year = requestId.substring(0, 4);
                        String month = requestId.substring(4,6);
                        String day = requestId.substring(6,8);
                        String hour = requestId.substring(8, 10);
                        String minute  = requestId.substring(10,12);
                        String endTime = ob.getString("receiveNotifyTime");
                        String starTime = requestId.substring(0, 17);

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                        double cost = result.equals("0000") ? sdf.parse(endTime).getTime() - sdf.parse(starTime).getTime() : 0;

                        // (日期，(订单，成功订单，订单金额，订单时长))
                        return new Tuple2<String[], Double[]>(new String[]{year,month,day, hour, minute}, new Double[]{1.0, sucess, fee, cost});
                    }
                }).filter(new Function<Tuple2<String[], Double[]>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String[], Double[]> tuple2) throws Exception {
                        if (tuple2 == null) {
                            return false;
                        }
                        return true;
                    }
                });


                final OffsetRange[] offsetRanges = atomicReference.get();

                KafkaOffsetUtil.saveCurrentOffset(offsetRanges);

                baseRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String[], Double[]>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String[], Double[]>> tuple2Iterator) throws Exception {

                        // 获取conn
                        Connection conn = MysqlPool.getInstance().getConnection();
                        // 关闭mysql自动提交
                        // conn.setAutoCommit(false);
                        String sql = "insert into mobileOperation.order_info(_year,_month,_day,_hour,_minute,_total,_success,_money,_time) values";
                        PreparedStatement ps = conn.prepareStatement(sql);

                        StringBuffer sb = new StringBuffer("");

                        while(tuple2Iterator.hasNext()){
                            Tuple2<String[],Double[]> tu = tuple2Iterator.next();
                            sb.append("('"+tu._1[0]+"','"+tu._1[1]+"','"+tu._1[2]+"','"+tu._1[3]+"','"+tu._1[4]+"',"+tu._2[0]
                                    +","+tu._2[1]+","+tu._2[2]+","+tu._2[3]+"),");
                        }
                        String exe_sql = sql + sb.substring(0, sb.length() - 1);
                        //   System.out.println(exe_sql+"=========");
                        //  ps.addBatch(exe_sql);

                        ps.executeUpdate(exe_sql);
                        //   conn.commit();
                        conn.close();

                    }
                });



            }
        });
    }
}
