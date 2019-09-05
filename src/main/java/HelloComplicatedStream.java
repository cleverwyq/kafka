import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class HelloComplicatedStream {
    private static final Properties streamProps = new Properties();
    public static void main(String[] args) {
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "hello_complicated_stream");
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream("lines", Consumed.with(Topology.AutoOffsetReset.EARLIEST));
        KStream<String, Long> wordsToCount = textLines
                .flatMapValues(value-> Arrays.asList(value.toLowerCase().split("\\W+")))
                .map((key, value) -> new KeyValue<String, String>(String.valueOf(value), String.valueOf(value)))
                .selectKey((key, value) -> key)
                .groupByKey()
                .count()
                .toStream()
                ;
        //wordsToCount.to("count_result");
        wordsToCount.
                print(Printed.toSysOut());

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
