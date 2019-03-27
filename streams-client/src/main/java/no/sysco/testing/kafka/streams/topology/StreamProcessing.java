package no.sysco.testing.kafka.streams.topology;

import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import no.sysco.testing.kafka.streams.utils.Tuple2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;

public class StreamProcessing implements Runnable {
  private final Properties properties;
  private final String topicIn;
  private final String topicOut;
  private final KafkaStreams kafkaStreams;

  public StreamProcessing(final Properties properties, final Tuple2<String, String> topics) {
    this.properties = properties;
    this.topicIn = topics._1;
    this.topicOut = topics._2;
    this.kafkaStreams = new KafkaStreams(topologyUpperCase(topics), properties);
  }

  // stateless
  public static Topology topologyUpperCase(final Tuple2<String, String> topics) {
    var streamsBuilder = new StreamsBuilder();
    var sourceStream = streamsBuilder.stream(topics._1, Consumed.with(Serdes.String(), Serdes.String()));
    sourceStream
        .mapValues((ValueMapper<String, String>) String::toUpperCase)
        .to(topics._2, Produced.with(Serdes.String(), Serdes.String()));
    return streamsBuilder.build();
  }

  // stateful
  public static Topology topologyCountAnagram(final Tuple2<String, String> topics, final String storeName) {
    var streamsBuilder = new StreamsBuilder();
    var sourceStream = streamsBuilder.stream(topics._1, Consumed.with(Serdes.String(), Serdes.String()));
    // 1. [null:"magic"] => ["acgim":"magic"]
    // 2. amount with same key
    sourceStream.map((key, value)-> {
      final var newKey = Stream.of(value.replaceAll(" ", "").split(""))
          .sorted()
          .collect(Collectors.joining());
      return KeyValue.pair(newKey, value);
    })
        .groupByKey()
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>> as(storeName))
        .toStream()
        .to(topics._2, Produced.with(Serdes.String(), Serdes.Long()));
    return streamsBuilder.build();
  }

  @Override public void run() {
    Optional.ofNullable(kafkaStreams).ifPresent(KafkaStreams::start);
  }

  public void close() {
    Optional.ofNullable(kafkaStreams).ifPresent(KafkaStreams::close);
  }
}