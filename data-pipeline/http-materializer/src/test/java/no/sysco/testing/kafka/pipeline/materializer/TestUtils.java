package no.sysco.testing.kafka.pipeline.materializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Properties;
import java.util.UUID;
import no.sysco.testing.kafka.pipeline.avro.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestUtils {

  public static KafkaProducer<String, Message> getMessageProducer(
      MaterializerConfig.KafkaConfig kafkaConfig) {
    return new KafkaProducer<>(
        getProperties(kafkaConfig.bootstrapServers, kafkaConfig.schemaRegistryUrl));
  }

  public static Properties getProperties(String bootstrapservers, String schemaUrl) {
    final Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    return properties;
  }
}
