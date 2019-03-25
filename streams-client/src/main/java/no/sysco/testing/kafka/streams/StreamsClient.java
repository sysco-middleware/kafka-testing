package no.sysco.testing.kafka.streams;

import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class StreamsClient {
  public static void main(String[] args) {
    final var id = "stream-client-app-id-1";
    final var properties = new Properties();
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, id);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    var inOutTopics = new Tuple2<>("in", "out");

    final var kafkaStreams = new KafkaStreams(topology(inOutTopics), properties);
    kafkaStreams.start();
  }

  public static Topology topology(final Tuple2<String, String> tuple) {
    var streamsBuilder = new StreamsBuilder();
    var sourceStream = streamsBuilder.stream(tuple._1, Consumed.with(Serdes.String(), Serdes.String()));
    // 1. [null:"magic"] => ["acgim":"magic"]
    // 2. amount with same key
    sourceStream.map((key, value)-> {
      final var newKey = Stream.of(value.replaceAll(" ", "")
          .split(""))
          .sorted()
          .collect(Collectors.joining());
      return KeyValue.pair(newKey, value);
    })
        .groupByKey()
        .count()
        .toStream()
        .to(tuple._2, Produced.with(Serdes.String(), Serdes.Long()));
    return streamsBuilder.build();
  }
}
