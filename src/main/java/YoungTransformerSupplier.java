import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class YoungTransformerSupplier implements TransformerSupplier {
    private final String storeName;

    public YoungTransformerSupplier(String storeName)
    {
        this.storeName = storeName;
    }

    @Override
    public Transformer get() {
        return new Transformer() {
            private KeyValueStore<String, String> stateStore;
            private ProcessorContext context;

            @Override
            @SuppressWarnings("unchecked")
            public void init(ProcessorContext context) {
                this.context = context;
                this.stateStore = (KeyValueStore<String, String>)(context.getStateStore(storeName));
                this.context.schedule(Duration.ofMinutes(5),
                        PunctuationType.STREAM_TIME,
                        (timestamp) ->  {
                            KeyValueIterator<String, String> it = this.stateStore.all();
                            while (it.hasNext()) {
                                it.next();
                            }
                            it.close();
                            this.context.commit();
                        }

                );
            }


            @Override
            public KeyValue<String, String> transform(Object key, Object value) {
                String url = String.valueOf(value);
                Integer count;
                String queryCount = this.stateStore.get(url);
                if (queryCount != null)
                {
                    count = Integer.parseInt(queryCount);
                    count ++;
                    System.out.println(url + " exec times: " + count.toString());
                    this.stateStore.put(url, count.toString());

                    if (count >= 3) {
                        this.stateStore.put(url, "0");

                        return KeyValue.pair(url + "-->", count.toString());
                    }
                }
                else
                {
                    this.stateStore.put(url, "1");
                    return null;
                }

                return null;
            }

            @Override
            public void close() {

            }
        };
    }
}
