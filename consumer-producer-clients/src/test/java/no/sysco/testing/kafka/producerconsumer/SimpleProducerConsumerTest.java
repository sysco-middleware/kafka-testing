package no.sysco.testing.kafka.producerconsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import no.sysco.testing.kafka.embedded.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SimpleProducerConsumerTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  @Test
  public void clusterIsRunning() {
    assertTrue(CLUSTER.isRunning());
  }

  @Test
  public void testSimpleProducer()
      throws InterruptedException, ExecutionException, TimeoutException {
    String topic = "topic1";
    CLUSTER.createTopic(topic);

    final KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties());

    // async with callback
    producer.send(
        new ProducerRecord<>(topic, "k1", "v1"),
        ((metadata, exception) -> {
          if (exception != null) fail();
        }));

    // sync
    final RecordMetadata recordMetadata =
        producer.send(new ProducerRecord<>(topic, "k2", "v2")).get(3, TimeUnit.SECONDS);

    assertTrue(recordMetadata.hasOffset());
    assertTrue(recordMetadata.hasTimestamp());
  }

  @Test
  public void testSimpleConsumer()
      throws InterruptedException, ExecutionException, TimeoutException {

    String topic = "topic2";
    CLUSTER.createTopic(topic);

    final KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties());
    producer.send(new ProducerRecord<>(topic, "k3", "v3")).get(1, TimeUnit.SECONDS);

    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties());
    consumer.subscribe(Collections.singletonList(topic));

    final ArrayList<String> values = new ArrayList<>();
    await()
        .atMost(15, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
              for (final ConsumerRecord<String, String> record : records)
                values.add(record.value());
              assertEquals(1, values.size());
            });
  }

  private Properties getProducerProperties() {
    final Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return properties;
  }

  private Properties getConsumerProperties() {
    final Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "client0");
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group0");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }
}
