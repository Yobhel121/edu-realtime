package com.yobhel.edu.realtime.util;

import com.yobhel.edu.realtime.common.EduConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 14:47
 **/
public class KafkaUtil {

    public static KafkaSource<String> getKafkaConsumer(String topic, String groupId) {

        return KafkaSource.<String>builder()
                .setBootstrapServers(EduConfig.KAFKA_BOOTSTRAPS)
                .setTopics(topic)
                .setGroupId(groupId)
                // 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                // 从最早位点开始消费
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null && bytes.length != 0) {
                            return new String(bytes);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
    }


    public static Sink<String> getKafkaProducer(String topic, String transId) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(EduConfig.KAFKA_BOOTSTRAPS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
    }
}
