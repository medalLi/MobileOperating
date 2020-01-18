package com.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @author medal
 * @create 2020-01-18 15:24
 **/
public class Jdbc_ops {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Jdbc_ops")
               // .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","centos");
        Dataset<Row> order_info = spark.read().jdbc("jdbc:mysql://localhost:3306/mobileOperation?useUnicode=true&characterEncoding=utf-8&useSSL=false"
                , "order_info", properties);
        order_info.show();

    }

}
