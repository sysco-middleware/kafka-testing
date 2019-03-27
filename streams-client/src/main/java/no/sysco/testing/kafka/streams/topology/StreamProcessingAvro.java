package no.sysco.testing.kafka.streams.topology;


import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import no.sysco.testing.kafka.streams.avro.Person;
import no.sysco.testing.kafka.streams.utils.Tuple2;
//import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

public class StreamProcessingAvro {

  // stateless
  public static Topology topologyUpperCase(final Tuple2<String, String> topics, final String schemaRegistryUrl) {
    final var builder = new StreamsBuilder();

    final var schema = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

    final Serde<Person> personSerdes = Serdes.serdeFrom(new SpecificAvroSerializer<Person>(), new SpecificAvroDeserializer<Person>());
    //final Serde<Person> personSerdes = new SpecificAvroSerde();

    personSerdes.configure(schema,false);
    final var sourceStream = builder.stream(topics._1, Consumed.with(Serdes.String(), personSerdes));
    sourceStream.foreach((k, v) -> System.out.printf("%s %s %s\n", v.getId(), v.getName(), v.getLastname()));
    sourceStream
        .mapValues(person -> Person.newBuilder()
            .setId(person.getId().toUpperCase())
            .setName(person.getName().toUpperCase())
            .setLastname(person.getLastname().toUpperCase())
            .build()
        )
        .to(topics._2, Produced.with(Serdes.String(), personSerdes));

    return builder.build();
  }

  // stateless vers 2
  public static Topology topologyUpperCase(final Tuple2<String, String> topics, final SchemaRegistryClient registryClient) {
    final var builder = new StreamsBuilder();
    final Serde<Person> personSerdes = new SpecificAvroSerde<>(registryClient);
    builder
        .stream(topics._1, Consumed.with(Serdes.String(), personSerdes))
        .foreach((k, v) -> System.out.printf("%s %s %s\n", v.getId(), v.getName(), v.getLastname()));

    return builder.build();
  }

}
