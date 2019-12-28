package com.utils;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;


/**
 * @author medal
 * @create 2019-12-28 13:05
 **/
public class KafkaOffsetUtil {
    public static Map<TopicPartition,Long> getMyCurrentOffset() throws Exception{
        Map<TopicPartition,Long> map = new HashMap<>();

        Connection connection = MysqlPool.getInstance().getConnection();
        String sql = "select * from mytest.kafkaOffset where topicName ='testTopic' and groupId = 'medalTestOffset'";
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while(resultSet.next()){
           String topic =  resultSet.getString("testTopic");
           int patition = resultSet.getInt("partitionId");
           long offset = resultSet.getLong("offset");
          // String groupId = resultSet.getString("groupId");

           map.put(new TopicPartition(topic,patition),offset);
        }
      //  TopicPartition topicPartition = new TopicPartition("",);
        return  map;
    }

    public static void saveCurrentOffset(OffsetRange[] offsetRanges) throws Exception{
        Connection connection = MysqlPool.getInstance().getConnection();
        //"REPLACE INTO `kafka_offset` (`topic`, `partition`, `offset`) VALUES (?,?,?)"
        String sql = "REPLACE INTO mytest.kafkaOffset (topicName, partitionId,offset,groupId) VALUES (?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);

        for(OffsetRange offsetRange : offsetRanges){
            String topic = offsetRange.topic();
            int partitionId = offsetRange.partition();
            long offset = offsetRange.untilOffset();
            ps.setString(1,topic);
            ps.setInt(2,partitionId);
            ps.setLong(3,offset);
            ps.setString(4,"medalTestOffset");
        }

        ps.executeUpdate();

    }
}
