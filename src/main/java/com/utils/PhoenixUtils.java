package main.java.com.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * @Author: ly-hjiab
 * @Description:
 * @Date :Created in 15:19 2019/8/29
 */
public class PhoenixUtils implements Serializable {


    private static Logger logger = LoggerFactory.getLogger(PhoenixUtils.class);
    private static Connection conn;

    static {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            Properties prop = new Properties();
            prop.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
           // conn = DriverManager.getConnection("Conf.URL", prop);
            conn = DriverManager.getConnection("jdbc:phoenix:hadoop02", prop);
        } catch (Exception e) {
            logger.info("phoenix的连接建立成功");
        }
    }


    public static Connection getConn() {
        synchronized (PhoenixUtils.class) {
            try {
                if (conn == null || conn.isClosed()) {
                    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                    Properties prop = new Properties();
                    prop.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
                    //conn = DriverManager.getConnection("Conf.URL", prop);
                    conn = DriverManager.getConnection("jdbc:phoenix:hadoop02", prop);
                }
            } catch (Exception e) {
                logger.info("phoenix的连接建立成功");
            }
        }
        return conn;
    }


    public static void close() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            logger.info("phoenix的连接关闭成功");
        }
    }
}
