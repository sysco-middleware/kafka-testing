package no.sysco.testing.kafka.streams.topology;

import no.sysco.testing.kafka.streams.avro.Person;
import no.sysco.testing.kafka.streams.utils.Tuple2;
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
      final Tuple2<String, String> topics, final Serde<Person> personSerdes) {
    final var builder = new StreamsBuilder();
    builder.stream(topics._1, Consumed.with(Serdes.String(), personSerdes))
        // .peek((k, v) -> System.out.printf("%s %s %s\n", v.getId(), v.getName(), v.getLastname()))
        .mapValues(
            person ->
                Person.newBuilder()
                    .setId(person.getId().toUpperCase())
                    .setName(person.getName().toUpperCase())
                    .setLastname(person.getLastname().toUpperCase())
                    .build())
        .to(topics._2, Produced.with(Serdes.String(), personSerdes));
    return builder.build();
  }

  // stateful
  public static Topology topologyCountUsersWithSameName(
      final Tuple2<String, String> topics,
      final Serde<Person> personSerdes,
      final String storeName) {

    final var builder = new StreamsBuilder();
    builder.stream(topics._1, Consumed.with(Serdes.String(), personSerdes))
        .groupBy((key, value) -> value.getName())
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(storeName))
        .toStream()
        .to(topics._2, Produced.with(Serdes.String(), Serdes.Long()));

    return builder.build();
  }
}
