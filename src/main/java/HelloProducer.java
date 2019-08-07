
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;


public class HelloProducer {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "demo-topic";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("client.id", "young_1");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //properties.put("request.timeout.ms", 30000122);
        //properties.put("metadata.max.age.ms", 30000000);
        properties.put("bootstrap.servers", brokerList);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "key1", "Hello Kafka");

        try {
            RecordMetadata rm = producer.send(record).get();
            System.out.println(rm.toString());
        }
        catch(Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
