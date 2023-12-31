package com.yobhel.edu.realtime.util;

import com.yobhel.edu.realtime.common.EduConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 14:47
 **/
public class KafkaUtil {

    //    public static KafkaSource<String> getKafkaConsumer(String topic, String groupId) {
//
//        return KafkaSource.<String>builder()
//                .setBootstrapServers(EduConfig.KAFKA_BOOTSTRAPS)
//                .setTopics(topic)
//                .setGroupId(groupId)
//                // 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点
////                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
//                // 从最早位点开始消费
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
//                    @Override
//                    public String deserialize(byte[] bytes) throws IOException {
//                        if (bytes != null && bytes.length != 0) {
//                            return new String(bytes);
//                        }
//                        return null;
//                    }
//
//                    @Override
//                    public boolean isEndOfStream(String s) {
//                        return false;
//                    }
//
//                    @Override
//                    public TypeInformation<String> getProducedType() {
//                        return TypeInformation.of(String.class);
//                    }
//                })
//                .build();
//    }
    public static KafkaSource<String> getKafkaConsumer(String topic, String groupId) {
        return KafkaSource.<String>builder()
                // 必要参数
                .setBootstrapServers(EduConfig.KAFKA_BOOTSTRAPS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null && message.length != 0) {
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                // 不必要的参数  设置offset重置的时候读取数据的位置
                // 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点
                //            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                // 从最早位点开始消费
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
    }


    public static KafkaSink<String> getKafkaProducer(String topic, String transId) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(EduConfig.KAFKA_BOOTSTRAPS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15 * 60 * 1000  + "")
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix(transId)
                .build();
    }

    public static <T> KafkaSink<T> getKafkaProducerBySchema(KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema, String transId) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(EduConfig.KAFKA_BOOTSTRAPS)
                .setRecordSerializer(kafkaRecordSerializationSchema)
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15 * 60 * 1000  + "")
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix(transId)
                .build();
    }

    public static String getKafkaDDL(String topic, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + EduConfig.KAFKA_BOOTSTRAPS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                // 从最早的偏移量开始
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                // 从特定的消费组偏移量开始
//                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'properties.auto.offset.reset' = 'earliest' , " +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getUpsertKafkaDDL(String topic) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + EduConfig.KAFKA_BOOTSTRAPS + "',\n" +
                "  'key.format' = 'json'," +
                "  'value.format' = 'json'" +
                ")";
    }

    public static void createTopicDb(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` string,\n" +
                "  `table` string,\n" +
                "  `type` STRING,\n" +
                "  `data` map<string,string>,\n" +
                "  `ts` string\n" +
                ")" + getKafkaDDL("topic_db", groupId));

    }


}
