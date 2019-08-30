import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class HelloStream {
    private static final Properties streamProps = new Properties();
    public static void main(String[] args) {
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "hello_stream");
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
//        final KeyValueMapper<Object, Object, KeyValue<Long,String>> mapper = new KeyValueMapper<Object, Object, KeyValue<Long,String>>() {
//
//            public KeyValue<Long,String> apply(Object key, Object value) {
//                return new KeyValue<Long, String>(Long.valueOf(String.valueOf(key)), String.valueOf(value));
//            }
//        };

        KeyValueMapper<Object , Object, KeyValue<String, String>> mapper = new KeyValueMapper<Object, Object, KeyValue<String, String> >() {
            @Override
            public KeyValue<String, String> apply(Object key, Object value) {
                return  new KeyValue<String, String>(String.valueOf(key)+ "&&", "hi");
            }
        };
        builder.stream("demo-topic").map(
                mapper
        ).to("demo-topic2");

        Topology tp = builder.build();

        final KafkaStreams streams = new KafkaStreams(tp, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
