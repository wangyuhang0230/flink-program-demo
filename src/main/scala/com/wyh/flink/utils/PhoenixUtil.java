package com.wyh.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author: WangYuhang
 * @create: 2021-03-31 11:06
 **/
public class PhoenixUtil {

    public static Connection getConnection(ParameterTool parameterTool) throws InterruptedException {

        Properties props = new Properties();
        props.setProperty("phoenix.functions.allowUserDefinedFunctions", "true");
        props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
        props.setProperty("phoenix.query.dateFormatTimeZone", "Asia/Shanghai");
        props.setProperty("phoenix.connection.schema", parameterTool.get("hn.phoenix.namespace"));

        int attemptCount = Integer.parseInt(parameterTool.get("hn.phoenix.attempt.count"));
        int attemptInterval = Integer.parseInt(parameterTool.get("hn.phoenix.attempt.interval"));
        Connection connection = null;
        while (attemptCount > 0 && connection == null) {
            try {
                connection = DriverManager.getConnection(parameterTool.get("hn.phoenix.url"), props);
                connection.setAutoCommit(false);
            } catch (Exception e) {
                Thread.sleep(attemptInterval * 1000);
                attemptCount--;
            }
        }
        if(connection == null) {
            throw new RuntimeException("获取连接失败，尝试次数 " + attemptCount + " 次，尝试间隔时间 " + attemptInterval + " 秒");
        }
        return connection;
    }

    public static void closeConn(PreparedStatement pres, Connection connection){
        if(pres != null){
            try {
                pres.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            pres = null;
        }

        if(connection != null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            connection = null;
        }

    }
}
