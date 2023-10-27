package com.yobhel.edu.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.yobhel.edu.realtime.common.EduConfig;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 17:17
 **/
public class PhoenixUtil {

    private static DruidDataSource druidDataSource = DruidDSUtil.getDruidDataSource();

    public static void executeDDL(String sqlString) {
        DruidPooledConnection connection = null;
        PreparedStatement preparedStatement = null;


        try {
            connection = druidDataSource.getConnection();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.out.println("连接池获取连接异常");
        }

        try {
            preparedStatement = connection.prepareStatement(sqlString);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("编译sql异常");
        }

        try {
            preparedStatement.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.out.println("建表语句错误");
        }

        // 关闭资源
        try {
            preparedStatement.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void executeDML(String sinkTable, JSONObject jsonObject) {
        StringBuilder sql = new StringBuilder();
        Set<Map.Entry<String, Object>> entries = jsonObject.entrySet();
        ArrayList<String> columns = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        StringBuilder symbols = new StringBuilder();
        for (Map.Entry<String, Object> entry : entries) {
            columns.add(entry.getKey());
            values.add(entry.getValue());
            symbols.append("?,");
        }

        sql.append("upsert into " + EduConfig.HBASE_SCHEMA + "." + sinkTable + "(");

        String columnsStrings = StringUtils.join(columns, ",");
        String symbolStr = symbols.substring(0, symbols.length() - 1).toString();
        sql.append(columnsStrings)
                .append(")values(")
                .append(symbolStr)
                .append(")");

        try {
            DruidPooledConnection connection = druidDataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
            for (int i = 0; i < values.size(); i++) {
                preparedStatement.setObject(i + 1, values.get(i) + "");
            }
            preparedStatement.executeUpdate();
            preparedStatement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
