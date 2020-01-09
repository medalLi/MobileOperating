package main.java.com.spark;

import com.alibaba.fastjson.JSONObject;
import com.bean.DaBean;
import com.bean.DaData;
import com.utils.HbaseUtil;
import com.utils.PropertiesUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

// 处理DA日志
public class HbaseSparkStreaming {
    private static Logger logger = LoggerFactory.getLogger(HbaseSparkStreaming.class);
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

            JavaInputDStream<ConsumerRecord<Object, Object>> messages =
                    KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
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
            JavaDStream<DaBean> commonBean = messages.mapPartitions(new FlatMapFunction<Iterator<ConsumerRecord<Object, Object>>, DaBean>() {
                public Iterator<DaBean> call(Iterator<ConsumerRecord<Object, Object>> consumerRecordIterator) throws Exception {
                    ArrayList<DaBean> list = new ArrayList<DaBean>();
                    while (consumerRecordIterator.hasNext()) {
                        String value = consumerRecordIterator.next().value().toString();
                        try {
                            DaBean bean = JSONObject.toJavaObject(JSONObject.parseObject(JSONObject.toJSON(value).toString()), DaBean.class);
                            list.add(bean);
                        } catch (Exception e) {
                            logger.error("数据不规范 : "+value);
                        }
                    }
                    return list.iterator();
                }
            });

            // 过滤空的对象
            JavaDStream<DaBean> commonBeanFilter = commonBean.filter(new Function<DaBean, Boolean>() {
                public Boolean call(DaBean daBean) throws Exception {
                    if (daBean != null) {
                        return true;
                    } else {
                        return false;
                    }
                }
            });

            // 准备入库
            commonBeanFilter.foreachRDD(new VoidFunction<JavaRDD<DaBean>>() {
                public void call(JavaRDD<DaBean> beanJavaRDD) throws Exception {
                    beanJavaRDD.foreachPartition(new VoidFunction<Iterator<DaBean>>() {
                        public void call(Iterator<DaBean> beanIterator) throws Exception {
                          //  SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            List<Put> list = new ArrayList<>();
                            while (beanIterator.hasNext()) {
                                DaBean daBean = beanIterator.next();
                                try {
                                    Put put = new Put(Bytes.toBytes(daBean.getData().getOTA_BU_BEHAVIOR_DA_ID()));
                                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("da_id"), Bytes.toBytes(daBean.getData().getDA_ID()));
                                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("event_id"), Bytes.toBytes(daBean.getData().getEVENT_ID()));
                                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("car_type"), Bytes.toBytes(daBean.getData().getCAR_TYPE()));
                                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("create_date"), Bytes.toBytes(daBean.getData().getCREATED_DATE()));
                                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("creator"), Bytes.toBytes(daBean.getData().getCREATOR()));
                                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("event_value"), Bytes.toBytes(daBean.getData().getEVENT_VALUE()));
                                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("last_update_date"), Bytes.toBytes(daBean.getData().getLAST_UPDATED_DATE()));
                                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("modifier"), Bytes.toBytes(daBean.getData().getMODIFIER()));
                                    list.add(put);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            HbaseUtil.put("default:userB", list);
                            list.clear();
                        }
                    });
                }
            });
    }
}





