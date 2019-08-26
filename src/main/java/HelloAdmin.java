import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;

public class HelloAdmin {
    public static void main(String[] args) throws  Throwable{
        AdminClient adminClient;
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        adminClient = AdminClient.create(properties);
        NewTopic nt = new NewTopic("new1", 1, (short)1);
        Collection<NewTopic> tpList = new ArrayList<NewTopic>();
        tpList.add(nt);

        //CreateTopicsResult result = adminClient.createTopics(tpList);
//        ListTopicsResult result = adminClient.listTopics();
//        for ( TopicListing r : result.listings().get())
//        {
//            System.out.println(r.toString());
//        }
        //System.out.println(result);

        DescribeTopicsResult r2 = adminClient.describeTopics(Arrays.asList("demo-topic"));
        for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : r2.values().entrySet()) {
            System.out.println("--------------");
            System.out.println(entry.getKey());
            TopicDescription tp = entry.getValue().get();
            System.out.println(tp.name());
            for(TopicPartitionInfo tpi : tp.partitions()) {
                System.out.println(tpi.leader());
            }
        }
    }
}
