package com.yobhel.edu.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yobhel.edu.realtime.bean.DimTableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 16:46
 **/
public class DimBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, DimTableProcess> tableProcessState;

    private HashMap<String, DimTableProcess> configMap = new HashMap<>();

    public DimBroadcastProcessFunction(MapStateDescriptor<String, DimTableProcess> tableProcessState) {
        this.tableProcessState = tableProcessState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop101:3306/edu_config?" +
                "user=root&password=123456&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");

        PreparedStatement preparedStatement = connection.prepareStatement("select * from edu_config.dwd_table_process");
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String columnValue = resultSet.getString(i);
                jsonObject.put(columnName, columnValue);
            }
            DimTableProcess dimTableProcess = jsonObject.toJavaObject(DimTableProcess.class);
            configMap.put(dimTableProcess.getSourceTable(), dimTableProcess);
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObject = JSON.parseObject(value);
        String type = jsonObject.getString("op");
        BroadcastState<String, DimTableProcess> tableConfigState = ctx.getBroadcastState(tableProcessState);
        if ("d".equals(type)) {
            DimTableProcess before = jsonObject.getObject("before", DimTableProcess.class);
            tableConfigState.remove(before.getSourceTable());
            configMap.remove(before.getSourceTable());
        } else {
            DimTableProcess after = jsonObject.getObject("after", DimTableProcess.class);

            tableConfigState.put(after.getSourceTable(), after);
        }
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, DimTableProcess> tableConfigState = ctx.getBroadcastState(tableProcessState);
        DimTableProcess tableProcess = tableConfigState.get(value.getString("table"));
        if (tableProcess == null) {
            tableProcess = configMap.get(value.getString("table"));
        }
        if (tableProcess != null) {
            String type = value.getString("type");
            if (type == null) {
                System.out.println("maxwell采集数据不完整");
            } else if (type.equals(tableProcess.getSourceTable())) {
                JSONObject data = value.getJSONObject("data");
                String sinkColumns = tableProcess.getSinkColumns();
                filterColumns(data,sinkColumns);

                data.put("sink_table",tableProcess.getSinkTable());
                data.put("ts",value.getLong("ts"));

                out.collect(data);
            }

        }

    }


    private void filterColumns(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        List<String> stringList = Arrays.asList(sinkColumns.split(","));
        entries.removeIf(entry -> !stringList.contains(entry.getKey()));
    }
}
