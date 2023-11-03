import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-11-02 20:13
 **/
public class FileToKafkaProducer {

    public static void main(String[] args) throws IOException {
//        sendData("/Users/yezhimin/Downloads/db_topic.txt", "topic_db");
        sendData("/Users/yezhimin/Downloads/log_topic/app.log", "topic_log");
    }

    public static void sendData(String filePath, String topic) throws IOException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker(s)
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath));
             Producer<String, String> producer = new KafkaProducer<>(properties)) {
            String line;
            while ((line = reader.readLine()) != null) {
                JSONObject value = JSON.parseObject(line);
                value.put("ts",System.currentTimeMillis()/1000);
//                System.out.println(value.toString());
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, value.toString());
                producer.send(record);
            }
        }
    }
}
