package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
 * 向kafka的某一个topic中生产数据
 *
 * 生产者的入口类就是Producer(接口)
 *      唯一的实现类KafkaProducer
 */
public class MyKafkaProducer {

    public static void main(String[] args) throws Exception {
        /*
            Each record consists of a key, a value, and a timestamp.
            Producer<K, V>
            K指的就是kafka topic中每一条消息中key的类型
            V指的就是kafka topic中每一条消息中value的类型
         */
        Properties properties = new Properties();
        properties.load(MyKafkaProducer.class.getClassLoader().getResourceAsStream("producer.properties"));
        Producer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);


        //发送数据 send
        ProducerRecord<Integer, String> record = null;
//        int start = 30;
//        int end = start + 10;
//        for (int i = start; i < end; i++) {
//            record = new ProducerRecord<Integer, String>("testTopic", i, i + " love you");
//            producer.send(record);
//        }
        BufferedReader br = new BufferedReader(new FileReader("E:\\WorkSpaces\\PracticalProject\\MobileOperating\\项目资料\\flumeLoggerapp4.log.20170412"));
        String line = null;
        while ((line = br.readLine()) != null){
            record = new ProducerRecord<Integer, String>("testTopic", line);
            producer.send(record);
            // Thread.sleep(100);
        }

        producer.close();
    }

}