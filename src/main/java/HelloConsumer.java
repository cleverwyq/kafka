import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;

//import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class HelloConsumer {
    public static final String brokerList="localhost:9092";
    //public static  String topic = "demo-topic";
    public static final String groupId = "demo.group";
    public static void main(String[] args) {

        Properties properties = new Properties();
        if (args.length > 1)
        {
            String clientid = args[0];
            if (clientid.length() >  0)
                properties.put("client.id", clientid);
        }

        String topic = args.length > 1 ? args[1]: "demo-topic";
        System.out.println("In java " + topic);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", brokerList);
        properties.put("max.block.ms", 6000000);
        properties.put("enable.auto.commit", true);
        //properties.put("heartbeat.interval.ms", 30000);
        properties.put("session.timeout.ms", 60000);
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

//            System.out.println("consumer.assignment");
//            Set<TopicPartition> assign = consumer.assignment();
//            for(TopicPartition tp : assign) {
//                System.out.println(tp.toString());
//            }
            //consumer.commitSync();

//            Map<String, List<PartitionInfo>> m = consumer.listTopics();
//            Map<TopicPartition, Long> n = consumer.endOffsets(Arrays.asList(new TopicPartition("demo-topic", 0)));
//
//            Map<TopicPartition, Long> tps = new ConcurrentHashMap<TopicPartition, Long>();
//            tps.put(new TopicPartition("demo-topic", 0), Long.valueOf(20000));
//            consumer.offsetsForTimes(tps);

            //List<PartitionInfo> l = consumer.partitionsFor("demo-topic");

            //TopicPartition tp = new TopicPartition("demo-topic", 0);
            //consumer.pause(Arrays.asList(tp));
            //long o = consumer.position(tp);
            //OffsetAndMetadata om = consumer.committed(tp);
            //consumer.close();
        }


    }
}
