package no.sysco.testing.kafka.pipeline.materializer.infrastructure.kafka;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import no.sysco.testing.kafka.pipeline.avro.Message;
import no.sysco.testing.kafka.pipeline.materializer.ApplicationConfig;
import no.sysco.testing.kafka.pipeline.materializer.domain.MessageRepresentationTransformer;
import no.sysco.testing.kafka.pipeline.materializer.infrastructure.service.DatabaseWebService;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

public class KafkaMessageMaterializer {
  private static final Logger log = Logger.getLogger(KafkaMessageMaterializer.class.getName());

  private final ApplicationConfig config;
  private final String sourceTopic;
  private final DatabaseWebService databaseWebService;
  private final KafkaStreams kafkaStreams;
  private final MessageRepresentationTransformer transformer;

  public KafkaMessageMaterializer(
      final ApplicationConfig applicationConfig,
      final DatabaseWebService databaseWebService,
      final MessageRepresentationTransformer transformer) {

    this.config = applicationConfig;
    this.databaseWebService = databaseWebService;
    this.transformer = transformer;
    this.sourceTopic = applicationConfig.getKafkaClientFactory().getSourceTopic();

    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationConfig.getName()+"steam-processing-v1");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, applicationConfig.getKafkaClientFactory().getBootstrapServers());
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, applicationConfig.getKafkaClientFactory().getSchemaRegistryUrl());
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    this.kafkaStreams = new KafkaStreams(topology(), properties);
  }

  private Topology topology() {
    final Map<String, String> schema = Collections
        .singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.getKafkaClientFactory().getSchemaRegistryUrl());
    final SpecificAvroSerde<Message> messageSerde = new SpecificAvroSerde<>();
    messageSerde.configure(schema,false);
    return topology(sourceTopic, messageSerde, databaseWebService, transformer);
  }

  static Topology topology(
      final String sourceTopic,
      final Serde<Message> messageSerdeValue,
      final DatabaseWebService databaseWebService,
      final MessageRepresentationTransformer transformer) {
    final StreamsBuilder builder = new StreamsBuilder();

    builder.stream(sourceTopic, Consumed.with(Serdes.String(), messageSerdeValue))
        .mapValues(transformer::transform)
        .foreach((id,message) -> databaseWebService.saveMessage(message));

    return builder.build();
  }
}
