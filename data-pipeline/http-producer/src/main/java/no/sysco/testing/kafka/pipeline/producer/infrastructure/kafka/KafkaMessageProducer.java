package no.sysco.testing.kafka.pipeline.producer.infrastructure.kafka;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import no.sysco.testing.kafka.pipeline.avro.Message;
import no.sysco.testing.kafka.pipeline.producer.ProducerRestConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaMessageProducer {
  private static final Logger log = Logger.getLogger(KafkaMessageProducer.class.getName());
  private final KafkaProducer<String, Message> producer;
  private final String sinkTopic;

  public KafkaMessageProducer(final ProducerRestConfig producerRestConfig) {
    Properties props = new Properties();
    props.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        producerRestConfig.getKafkaClientFactory().getBootstrapServers());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, producerRestConfig.getName() + "-producer-id1");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    // props.put(ProducerConfig.RETRIES_CONFIG, 0);
    // props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    // props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        producerRestConfig.getKafkaClientFactory().getSchemaRegistryUrl());
    this.sinkTopic = producerRestConfig.getKafkaClientFactory().getSinkTopic();
    this.producer = new KafkaProducer<>(props);
  }

  public void producerMessage(final Message message) {
    final ProducerRecord<String, Message> record =
        new ProducerRecord<>(sinkTopic, message.getId(), message);
    producer.send(
        record,
        ((metadata, exception) -> {
          if (exception == null) {
            Map<String, Object> data = new HashMap<>();
            data.put("topic", metadata.topic());
            data.put("partition", metadata.partition());
            data.put("offset", metadata.offset());
            data.put("timestamp", metadata.timestamp());
            log.info(data.toString());
          } else {
            exception.printStackTrace();
          }
        }));
  }
}
