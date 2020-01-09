package main.java.com.spark;

import com.alibaba.fastjson.JSONObject;
import com.bean.DaBean;
import com.bean.DaData;
import com.utils.KakfaOffsetUtils;
import com.utils.PhoenixUtils;
import com.utils.PropertiesUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;

// 处理DA日志
public class HbaseSparkStreaming_phoenix_withOffset {
    private static Logger logger = LoggerFactory.getLogger(HbaseSparkStreaming_phoenix_withOffset.class);
    private static final String GROUP_ID = "medal_new_da";
    private static final String TRACK_TOPIC = "medal_DA1";

    private static final String broker_list = "hadoop04:9092,hadoop05:9092,hadoop06:9092";

    public static void main(String[] args) {
        try {
            //设置spark配置
            SparkConf sparkConf = new SparkConf().setAppName("HbaseSparkStreaming");
           //  sparkConf.setMaster("local[*]");

            // spark基本配置信息
            // 如果为true，Spark会StreamingContext在JVM关闭时正常关闭，而不是立即关闭。
            sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
            //使用新的Kafka直接流API时，将从每个Kafka分区读取数据的最大速率（每秒的记录数）。
            sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "350");  // Kafka每个分区每次最多10条
            // 并行度
            sparkConf.set("spark.default.parallelism", "6");
            // 开启反压机制
            sparkConf.set("spark.streaming.backpressure.enabled", "true");
            sparkConf.set("spark.streaming.kafka.consumer.poll.ms", "10000");
            // 如果设置为“ true”，则执行任务的推测执行。这意味着如果一个或多个任务在一个阶段中运行缓慢，它们将被重新启动。
            sparkConf.set("spark.speculation", "true");
            // Spark多久检查一次要推测的任务。默认是100毫秒
            sparkConf.set("spark.speculation.interval", "300s");
            //在特定阶段启用推测之前必须完成的部分任务。默认是0.75
            sparkConf.set("spark.speculation.quantile", "0.7");
            //是否使用动态资源分配，该资源分配将根据工作负载上下扩展在此应用程序中注册的执行程序的数量。默认是false
            // sparkConf.set("spark.dynamicAllocation.enabled","false");
            // spark.streaming.backpressure.enabled

            // 将自定义的对象使用Kryo序列化，加快对象在网络上传输速度
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            sparkConf.registerKryoClasses(new Class[]{DaBean.class,
                    DaData.class
            });
            //初始化spark上下文
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
            //配置kafka
            Map<String, Object> kafkaParams = PropertiesUtils.getkafkaParams(GROUP_ID, broker_list);
            final HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(TRACK_TOPIC));

            // 获取偏移量
            Map<TopicPartition, Long> offset = KakfaOffsetUtils.getOffset(TRACK_TOPIC, GROUP_ID);

            JavaInputDStream<ConsumerRecord<Object, Object>> messages =
                    KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(topicsSet, kafkaParams,offset));
            //数据入库
            saveData(messages);

            jssc.start();
            jssc.awaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 数据入库的具体实现
    private static void saveData(JavaInputDStream<ConsumerRecord<Object, Object>> messages) {
            //转换格式，json转换为java bean
        messages.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<Object, Object>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<Object, Object>> consumerRecordJavaRDD) throws Exception {
                //获取最新的偏移量
                OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();

                JavaRDD<DaBean> commonBean = consumerRecordJavaRDD.mapPartitions(new FlatMapFunction<Iterator<ConsumerRecord<Object, Object>>, DaBean>() {
                    public Iterator<DaBean> call(Iterator<ConsumerRecord<Object, Object>> consumerRecordIterator) throws Exception {
                        ArrayList<DaBean> list = new ArrayList<DaBean>();
                        while (consumerRecordIterator.hasNext()) {
                            String value = consumerRecordIterator.next().value().toString();
                            try {
                                DaBean bean = JSONObject.toJavaObject(JSONObject.parseObject(JSONObject.toJSON(value).toString()), DaBean.class);
                                list.add(bean);
                            } catch (Exception e) {
                                logger.error("数据不规范 : " + value);
                            }
                        }
                        return list.iterator();
                    }
                });

                // 过滤空的对象
                JavaRDD<DaBean> commonBeanFilter = commonBean.filter(new Function<DaBean, Boolean>() {
                    public Boolean call(DaBean daBean) throws Exception {
                        if (daBean != null) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });

                // 准备入库
                commonBeanFilter.foreachPartition(new VoidFunction<Iterator<DaBean>>(){
                    public void call (Iterator < DaBean > beanIterator) throws Exception {
                        Connection conn = PhoenixUtils.getConn();
                        String sql = "upsert into USERB (OTA_BU_BEHAVIOR_DA_ID,DA_ID ,EVENT_ID" +
                                ",CAR_TYPE,CREATED_DATE,CREATOR,EVENT_VALUE,LAST_UPDATED_DATE,MODIFIER) values(?,?,?,?,?,?,?,?,?)";
                        PreparedStatement ps = conn.prepareStatement(sql);
                        while (beanIterator.hasNext()) {
                            DaBean daBean = beanIterator.next();
                            ps.setString(1, daBean.getData().getOTA_BU_BEHAVIOR_DA_ID());
                            ps.setString(2, daBean.getData().getDA_ID());
                            ps.setString(3, daBean.getData().getEVENT_ID());
                            ps.setString(4, daBean.getData().getCAR_TYPE());
                            ps.setString(5, daBean.getData().getCREATED_DATE());
                            ps.setString(6, daBean.getData().getCREATOR());
                            ps.setString(7, daBean.getData().getEVENT_VALUE());
                            ps.setString(8, daBean.getData().getLAST_UPDATED_DATE());
                            ps.setString(9, daBean.getData().getMODIFIER());
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        conn.commit();
                        // 入库完毕，提交kafka偏移量
                        for(OffsetRange offset :offsetRanges){
                            KakfaOffsetUtils.saveOffset(TRACK_TOPIC,offset.partition(),offset.untilOffset(),GROUP_ID);
                        }
                    }
                });
            }
        });
    }
}





