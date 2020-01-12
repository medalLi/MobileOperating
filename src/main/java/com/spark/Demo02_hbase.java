package com.spark;

import com.alibaba.fastjson.JSONObject;
import com.utils.HbaseUtil;

import com.utils.KafkaOffsetUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * https://blog.csdn.net/qq_16415417/category_7973982.html
 *
 * @author medal
 * @create 2019-11-10 11:33
 **/
public class Demo02_hbase {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args)  {
        try {
        SparkConf sparkConf = new SparkConf().setAppName("Demo02_hbase");
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

        jssc.sparkContext().setLogLevel("INFO");


        Collection<String> topics = Arrays.asList("testTopic");

        LongAccumulator right_json = jssc.sparkContext().sc().longAccumulator();
        LongAccumulator error_json = jssc.sparkContext().sc().longAccumulator();
        // 获取偏移量
            Map<TopicPartition,Long> myOffset = KafkaOffsetUtil.getMyCurrentOffset();
            JavaInputDStream<ConsumerRecord<String, String>> messages = null;

//            try {
//                messages =
//                        KafkaUtils.createDirectStream(
//                                jssc,
//                                LocationStrategies.PreferConsistent(),
//                                ConsumerStrategies.Subscribe(topics, kafkaParams,myOffset)
//                        );
//            } catch (Exception e) { // 如果手动维护偏移量失败，则自动维护偏移量
               // e.printStackTrace();
                messages =
                        KafkaUtils.createDirectStream(
                                jssc,
                                LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.Subscribe(topics, kafkaParams)
                        );

                System.out.println("手动维护偏移量！》》》》》》》》》》》》》》》》》》》》》》》");
  //          }

         messages.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
             @Override
             public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD) throws Exception {
                 OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();

                 JavaRDD<String> lines = consumerRecordJavaRDD.map(new Function<ConsumerRecord<String, String>, String>() {
                     @Override
                     public String call(ConsumerRecord<String, String> consumerRecord) throws Exception {

                         return consumerRecord.value();
                     }
                 });

                 //  lines.print();
                 JavaRDD<JSONObject> filter = lines.map(new Function<String, JSONObject>() {

                     public JSONObject call(String s) throws Exception {
                         JSONObject ob = null;
                         try {
                             ob = JSONObject.parseObject(s);
                             right_json.add(1);
                         } catch (Exception e) {
                             error_json.add(1);
                             // e.printStackTrace();
                         }
                         return ob;
                     }

                 }).filter(new Function<JSONObject, Boolean>() {
                     @Override
                     public Boolean call(JSONObject ob) throws Exception {
                         return ob.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq");
                     }
                 });

                 // rdd 转换完成
                 insertMysql(filter,offsetRanges);

             }
         });

            jssc.start();
            jssc.awaitTermination();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static  void insertMysql(JavaRDD<JSONObject> filter,OffsetRange[] offsetRanges) {
        //持久化数据
        filter.persist(StorageLevel.MEMORY_ONLY());

        long num = filter.count();
        if (num == 0) {
            return;
        }
        // stringJavaRDD.coalesce(6);

        JavaPairRDD<String[], Double[]> baseRDD = filter.mapToPair(new PairFunction<JSONObject, String[], Double[]>() {
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

        baseRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String[], Double[]>>>() {
            @Override
            public void call(Iterator<Tuple2<String[], Double[]>> tuple2Iterator) throws Exception {
                List<Put> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String[],Double[]> tu = tuple2Iterator.next();
                    try {
                        Put put = new Put(Bytes.toBytes(tu._2[3]));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("_year"), Bytes.toBytes(tu._1[0]));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("_month"), Bytes.toBytes(tu._1[1]));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("_day"), Bytes.toBytes(tu._1[2]));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("_hour"), Bytes.toBytes(tu._1[3]));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("_minute"), Bytes.toBytes(tu._1[4]));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("_total"), Bytes.toBytes(tu._2[0]));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("_success"), Bytes.toBytes(tu._2[1]));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("_money"), Bytes.toBytes(tu._2[2]));
                        list.add(put);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
              HbaseUtil.put("default:order_info", list);
                list.clear();

            }
        });
//
//        // 计算完毕，提交偏移量
//        try {
//            KafkaOffsetUtil.saveCurrentOffset(offsetRanges);
//        } catch (Exception e) {
//
//            System.out.println("存储偏移量异常！");
//            e.printStackTrace();
//        }

    }
}
