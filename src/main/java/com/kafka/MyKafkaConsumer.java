package com.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * kafka consumer的api操作
 * 从topic-hadoop中消费数据
 * consumer详细配置信息，参见：http://kafka.apache.org/documentation.html#consumerconfigs
 *
 * 某一个消费者组的消费的偏移量都被记录在__consumer_offsets的topic中，比如上一次消费者组group，
 * 消费topic hadoop的三个partition的偏移量分别为(0, 11), (1, 12), (2, 3)
 *  下一次在进行消费的时候，对应的partition就从相关的偏移量的位置开始消费(0, 12), (1, 13), (2, 4)
 */
public class MyKafkaConsumer {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.load(MyKafkaConsumer.class.getClassLoader().getResourceAsStream("consumer.properties"));
        Consumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(properties);

        //订阅某些topic
        consumer.subscribe(Arrays.asList("testTopic".split(",")));
        //从kafka中拉取数据
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(1000);

            for (ConsumerRecord<Integer, String> record : records) {
                String topic = record.topic();
                int partition = record.partition();
                Integer key = record.key();
                String value = record.value();
                long offset = record.offset();
                System.out.println(String.format(
                        "topic: %s\tpartition: %d\toffset: %d\tkey: %d\tvalue: %s",
                        topic,
                        partition,
                        offset,
                        key,
                        value
                ));
            }
        }

//        consumer.close();
    }
}
