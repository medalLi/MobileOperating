package com.utils;


import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;

import java.beans.PropertyVetoException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Function MySQL连接通用类
 */
public class MysqlPool implements Serializable {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(MysqlPool.class);
    private static MysqlPool pool;
    private static ComboPooledDataSource cpds;

//    private MysqlPool() {
//    }

    private MysqlPool() {
        cpds = new ComboPooledDataSource(true);

        try {
            cpds.setDriverClass("com.mysql.jdbc.Driver");
            // 本地环境
            //cpds.setJdbcUrl("jdbc:mysql://localhost:3306/mobileOperation?useUnicode=true&characterEncoding=utf-8&useSSL=false");
            // 集群环境
            cpds.setJdbcUrl("jdbc:mysql://cdh.medal.com:3306/mobileOperation?useUnicode=true&characterEncoding=utf-8&useSSL=false");
            cpds.setUser("root");
            cpds.setPassword("centos");
            cpds.setInitialPoolSize(5);
            cpds.setMinPoolSize(3);
            cpds.setMaxPoolSize(10);
            cpds.setAcquireIncrement(5);
            cpds.setMaxStatements(50);
            cpds.setMaxIdleTime(3600);
            cpds.setIdleConnectionTestPeriod(120);
        } catch (PropertyVetoException e) {
            logger.error(e.getMessage());
        }
    }

    public final static MysqlPool getInstance() {

        synchronized(MysqlPool.class) {
            try {
                if (pool == null) {
                    pool = new MysqlPool();
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

        return pool;
    }

    public synchronized final Connection getConnection() {
        Connection conn = null;
        try {
            conn = cpds.getConnection();
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }

        return conn;
    }

    public static void release(Connection conn, PreparedStatement pstmt) {
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static void releaseStat(Connection conn, Statement stm) {
        if (stm != null){
            try{
                stm.close();
            }catch(Exception e){
                logger.error(e.getMessage());
            }
        }

        if (conn != null){
            try{
                conn.close();
            }catch(Exception e){
                logger.error(e.getMessage());
            }
        }
    }

}