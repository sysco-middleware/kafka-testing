package no.sysco.testing.kafka.streams.topology;

import no.sysco.testing.kafka.streams.avro.Person;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class StreamProcessingAvro {

  // stateless
  public static Topology topologyUpperCase(
      final String sourceTopic,
      final String sinkTopic,
      final Serde<Person> personSerdes) {
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream(sourceTopic, Consumed.with(Serdes.String(), personSerdes))
        // .peek((k, v) -> System.out.printf("%s %s %s\n", v.getId(), v.getName(), v.getLastname()))
        .mapValues(
            person ->
                Person.newBuilder()
                    .setId(person.getId().toUpperCase())
                    .setName(person.getName().toUpperCase())
                    .setLastname(person.getLastname().toUpperCase())
                    .build())
        .to(sinkTopic, Produced.with(Serdes.String(), personSerdes));
    return builder.build();
  }

  // stateful
  public static Topology topologyCountUsersWithSameName(
      String sourceTopic,
      String sinkTopic,
      final Serde<Person> personSerdes,
      final String storeName) {

    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream(sourceTopic, Consumed.with(Serdes.String(), personSerdes))
        .groupBy((key, value) -> value.getName())
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(storeName))
        .toStream()
        .to(sinkTopic, Produced.with(Serdes.String(), Serdes.Long()));

    return builder.build();
  }
}
