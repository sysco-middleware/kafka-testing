package no.sysco.testing.kafka.producerconsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public final class AvroEventProducer {
  private final Properties properties;
  private KafkaProducer<String, String> producer;
  private String topicName;
  private static final Logger logger = Logger.getLogger(AvroEventProducer.class.getName());

  public AvroEventProducer(Properties properties, String topicName) {
    this.properties = properties;
    this.topicName = topicName;
    this.producer = new KafkaProducer<String, String>(properties);
  }

  public void produceWithAsyncAck(String key, String value) {
    // no key
    final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
    producer.send(
        record,
        (metadata, exception) -> {
          if (exception == null) {
            Map<String, Object> data = new HashMap<>();
            data.put("topic", metadata.topic());
            data.put("partition", metadata.partition());
            data.put("offset", metadata.offset());
            data.put("timestamp", metadata.timestamp());
            logger.info(data.toString());
          } else {
            logger.log(Level.SEVERE, "Producer failed to send: {}", exception);
            exception.printStackTrace();
          }
        });
  }

  public RecordMetadata produceWithSyncAck(String key, String value)
      throws InterruptedException, ExecutionException, TimeoutException {
    final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
    return producer.send(record).get(1, TimeUnit.SECONDS);
  }

  public KafkaProducer<String, String> getProducer() { return producer; }
}
