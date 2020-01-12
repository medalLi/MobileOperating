package com.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 */
public class HbaseUtil {

    private static Logger logger = LoggerFactory.getLogger(HbaseUtil.class);

    private static Configuration hbaseConfiguration = null;

    private static Connection hbaseConnection = null;

    static {
        try {
            hbaseConnection = getHBaseConnection();
            logger.info("创建HbaseConfiguration、HConnection成功!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 数据写入Hbase.
     *
     * @param tableName
     * @param puts
     * @return
     * @throws Exception
     */
    public static int put(String tableName, List<Put> puts) throws IOException {
        int total = puts.size();
        BufferedMutator table = null;
        BufferedMutatorParams params;
        //tableName = tableName.toUpperCase();
        try {
            final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
                @Override
                public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                    for (int i = 0; i < e.getNumExceptions(); i++) {
                        logger.error("Failed to sent put " + e.getRow(i) + ".");
                    }
                }
            };
            params = new BufferedMutatorParams(TableName.valueOf(tableName))
                    .listener(listener);
            params.writeBufferSize(10 * 1024 * 1024);// 可以自己设定阈值 5M 达到5M则提交一次.
            if (hbaseConnection == null) {
                hbaseConnection = getHBaseConnection();
            }
            table = hbaseConnection.getBufferedMutator(params);
            table.mutate(puts);
            table.flush();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("写入Hbase异常");
        } finally {
            if (table != null) {
                table.close();
            }
        }
        return total;
    }

    /**
     * 数据写入Hbase.
     *
     * @param tableName
     * @param puts
     * @return
     * @throws Exception
     */
    public static void put(String tableName, List<Put> puts, Connection conn) throws IOException {
        BufferedMutator table = null;
        BufferedMutatorParams params;
        tableName = tableName.toUpperCase();
        try {
            final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
                @Override
                public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                    for (int i = 0; i < e.getNumExceptions(); i++) {
                        logger.error("Failed to sent put " + e.getRow(i) + ".");
                    }
                }
            };
            params = new BufferedMutatorParams(TableName.valueOf(tableName))
                    .listener(listener);
            params.writeBufferSize(10 * 1024 * 1024);// 可以自己设定阈值 5M 达到5M则提交一次.
            table = conn.getBufferedMutator(params);
            table.mutate(puts);
            table.flush();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("写入Hbase异常");
        }
    }

    /**
     * 构造Conf.
     *
     * @return
     */
    private static Configuration createConf() {
//        Properties props = new PropertiesUtil("common.properties").getProperties();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","cdh.medal.com");//使用eclipse时必须添加这个，否则无法定位
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return conf;
    }

    /**
     * 关闭连接.
     */
    public static void close() {
        try {
            if (hbaseConnection != null) {
                hbaseConnection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Connection getHBaseConnection() {
//            Properties props = new PropertiesUtil("common.properties").getProperties();
        hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.set("hbase.zookeeper.quorum", "cdh.medal.com");//使用eclipse时必须添加这个，否则无法定位
        hbaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            hbaseConnection = ConnectionFactory.createConnection(hbaseConfiguration);
            logger.info("获取hbase 连接成功!!");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("获取hbase 连接异常！");
            hbaseConnection = null;
        }

        return hbaseConnection;
    }
}
