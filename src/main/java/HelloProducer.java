
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class HelloProducer {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "demo-topic";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", brokerList);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "Hello Kafka");

        try {
            producer.send(record);
        }
        catch(Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
