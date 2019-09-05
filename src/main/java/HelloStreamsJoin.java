import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class HelloStreamsJoin {
    private static final Properties streamProps = new Properties();
    public static void main(String[] args) {

        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "hello_join_stream");
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputLeft = builder.stream("input_left");
        KStream<String, String> inputRight = builder.stream("input_right");

        KStream<String, String> all = inputLeft.selectKey((k, v) -> v.split(",")[1])
                .join(inputRight.selectKey((k, v) -> v.split(",")[0]),
                        new ValueJoiner<String, String, String>() {
                            @Override
                            public String apply(String value1, String value2) {
                                return value1 + " <----> " + value2;
                            }
                        },
                        JoinWindows.of(30000)
                );

        all.print(Printed.toSysOut());

        Topology tp = builder.build();

        KafkaStreams streams = new KafkaStreams(tp, streamProps);
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
