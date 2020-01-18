package com.sql;

import org.apache.spark.sql.SparkSession;

/**
 * @author medal
 * @create 2020-01-18 15:22
 **/
public class Hive_ops {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Hive_ops")
                .enableHiveSupport()
                // .config("spark.some.config.option", "some-value")
                .getOrCreate();
       // String sql = "create database myTest";
        String sql = "create table if not exists mytest.student2(\n" +
                "id int, name string\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t'\n" +
                "stored as textfile\n" +
                "location '/user/hive/warehouse/mytest.db/student2'";
        spark.sql(sql);
    }
}
