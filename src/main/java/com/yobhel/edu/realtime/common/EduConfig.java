package com.yobhel.edu.realtime.common;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 14:51
 **/
public class EduConfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "EDU_REALTIME";

//    // Phoenix驱动
//    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
//
//    // Phoenix连接参数
//    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181";
//
//    // kafka连接地址
//    public static final String KAFKA_BOOTSTRAPS = "hadoop101:9092,hadoop102:9092,hadoop103:9092";
//
//    // ClickHouse 驱动
//    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
//
//    // ClickHouse 连接 URL
//    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop101:8123/edu_realtime";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:localhost:2181";

    // kafka连接地址
    public static final String KAFKA_BOOTSTRAPS = "localhost:9092";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
//    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop101:8123/edu_realtime";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://15.tcp.cpolar.top:13533/edu_realtime";

}