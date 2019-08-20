import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;

//import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


public class HelloConsumer {
    public static final String brokerList="localhost:9092";
    public static final String topic = "demo-topic";
    public static final String groupId = "demo.group";
    public static void main(String[] args) {

        Properties properties = new Properties();
        if (args.length > 1)
        {
            String clientid = args[1];
            if (clientid.length() >  0)
                properties.put("client.id", clientid);
        }


        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", brokerList);
        properties.put("max.block.ms", 6000000);
        properties.put("enable.auto.commit", false);

        properties.put("group.id", groupId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        List<TopicPartition> list = new ArrayList<TopicPartition>();
        list.add(new TopicPartition(topic, 0));

        //consumer.assign(list);
        //consumer.seekToBeginning(list);
        //consumer.seek(new TopicPartition(topic, 0), 8);
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll((10000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }

            //consumer.commitSync();
        }


    }
}
