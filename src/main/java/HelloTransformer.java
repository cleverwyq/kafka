import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class HelloTransformer {
    private static final Properties streamProps = new Properties();
    public static void main(String[] args) {
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "hello_transformer");
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("young_state_store"),
                Serdes.String(),
                Serdes.String()
        ).withCachingEnabled();
        builder.addStateStore(storeBuilder);

        KStream<String, String> inputStream = builder.stream("young_transform_before");
        KStream<String, String> transformStream = inputStream.transform(new YoungTransformerSupplier(storeBuilder.name()),
                storeBuilder.name()

        );
        transformStream.to("young_transform_after");

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
