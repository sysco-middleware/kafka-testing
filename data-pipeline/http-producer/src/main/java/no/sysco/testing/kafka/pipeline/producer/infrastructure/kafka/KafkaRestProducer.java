package no.sysco.testing.kafka.pipeline.producer.infrastructure.kafka;

import java.util.Properties;
import no.sysco.testing.kafka.pipeline.avro.Message;
import no.sysco.testing.kafka.pipeline.producer.ApplicationConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class KafkaRestProducer {
  private final KafkaProducer<String, Message> producer;
  private static final Logger log = Logger.getLogger(KafkaRestProducer.class.getName());

  public KafkaRestProducer(final ApplicationConfig applicationConfig) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, applicationConfig.getKafkaClientFactory().getBootstrapServers());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, applicationConfig.getName() + "-producer-id1");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
  }
}
