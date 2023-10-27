import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.Properties;
import java.util.Set;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-26 17:46
 **/
public class KafkaTopicLister {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "16.tcp.cpolar.top:13837"); // Kafka服务器地址

        AdminClient adminClient = AdminClient.create(properties);

        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true); // 包括内部主题

        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        KafkaFuture<Set<String>> topicNames = listTopicsResult.names();

        try {
            Set<String> topics = topicNames.get();
            System.out.println("Kafka Topics:");
            for (String topic : topics) {
                System.out.println(topic);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
    }
}
