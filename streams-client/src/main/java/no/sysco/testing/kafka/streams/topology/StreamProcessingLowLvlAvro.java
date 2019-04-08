package no.sysco.testing.kafka.streams.topology;

import no.sysco.testing.kafka.streams.avro.Person;
import no.sysco.testing.kafka.streams.utils.Tuple2;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

public class StreamProcessingLowLvlAvro {

  // stateful
  public static Topology topologyDedupByUserId(
      final Tuple2<String, String> topics,
      final Serde<Person> personSerdes,
      final String idStore) {

    final StreamsBuilder builder = new StreamsBuilder();
    builder
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(idStore),
            Serdes.String(),
            personSerdes))
        .stream(topics._1, Consumed.with(Serdes.String(), personSerdes))
        .transform(()->new Transformer<String, Person, KeyValue<String, Person>>() {
          KeyValueStore<String, Person> stateStore;
          @Override public void init(ProcessorContext context) {
            this.stateStore = (KeyValueStore<String, Person>) context.getStateStore(idStore);
          }
          @Override public KeyValue<String, Person> transform(String key, Person value) {
            String id = value.getId();
            if (!id.equals(key)) return null; // some weird

            Person person = stateStore.get(key);
            if (person == null) {
              // add to store
              stateStore.put(key, value);
              return KeyValue.pair(key, value);
            } else { return null; }
          }
          @Override public void close() { }
        }, idStore)
        .to(topics._2, Produced.with(Serdes.String(), personSerdes));

    return builder.build();
  }
}
