package com.yobhel.edu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.yobhel.edu.realtime.app.func.DimBroadcastProcessFunction;
import com.yobhel.edu.realtime.app.func.DimPhoenixSinkFunc;
import com.yobhel.edu.realtime.bean.DimTableProcess;
import com.yobhel.edu.realtime.util.EnvUtil;
import com.yobhel.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 15:03
 **/
public class DimSinkApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        DataStreamSource<String> eduDS = env.fromSource(KafkaUtil.getKafkaConsumer("topic_db", "dim_sink_app"), WatermarkStrategy.noWatermarks(), "kafka_source");
        SingleOutputStreamOperator<JSONObject> jsonDS = eduDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if (!(type.equals("bootstrap-complete") || type.equals("bootstrap-start"))) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("数据转换json错误");
                }
            }
        });

        // TODO 4 使用flinkCDC读取配置表数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("edu_config")
                .tableList("edu_config.table_process")
                // 定义读取数据的格式
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 设置读取数据的模式
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> configDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");

        configDS.print();

        MapStateDescriptor<String, DimTableProcess> tableProcessState = new MapStateDescriptor<>("table_process_state", String.class, DimTableProcess.class);

        BroadcastStream<String> broadcastStream = configDS.broadcast(tableProcessState);

        BroadcastConnectedStream<JSONObject, String> connectCS = jsonDS.connect(broadcastStream);

        SingleOutputStreamOperator<JSONObject> dimDS = connectCS.process(new DimBroadcastProcessFunction(tableProcessState));

        dimDS.addSink(new DimPhoenixSinkFunc());

        env.execute();


    }
}
