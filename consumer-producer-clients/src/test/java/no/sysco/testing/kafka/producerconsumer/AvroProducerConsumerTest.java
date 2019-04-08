package no.sysco.testing.kafka.producerconsumer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.time.Duration;
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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AvroProducerConsumerTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String topic = "topic";

  @BeforeClass
  public static void createTopics() throws Exception {
    // CLUSTER.start();
    CLUSTER.createTopic(topic);
  }

  @Before
  public void setup() {
    // start streams
  }

  @After
  public void closeStreams() {
    // close streams
  }

  @Test
  public void clusterIsRunning() {
    assertTrue(CLUSTER.isRunning());
  }

  @Test
  public void testProducer()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {

    final KafkaProducer<String, Event> producer = new KafkaProducer<>(getProducerProperties());
    // async with callback
    producer.send(
        new ProducerRecord<>(topic, "id-1", Event.newBuilder().setId("id-1").setType("type-1").setContext("context-1").build()),
        ((metadata, exception) -> {
          if (exception == null) {
            System.out.println("SUCCESS bro");
          } else {
            fail();
          }
        }));

    // sync
    final RecordMetadata recordMetadata =
        producer.send( new ProducerRecord<>(topic, "id-2", Event.newBuilder().setId("id-2").setType("type-2").setContext("context-2").build())).get(3, TimeUnit.SECONDS);

    assertTrue(recordMetadata.hasOffset());
    assertTrue(recordMetadata.hasTimestamp());
  }

  @Test
  public void testSimpleConsumer()
      throws InterruptedException, ExecutionException, TimeoutException {

    final KafkaProducer<String, Event> producer = new KafkaProducer<>(getProducerProperties());
    final RecordMetadata recordMetadata =
        producer.send( new ProducerRecord<>(topic, "id-3", Event.newBuilder().setId("id-3").setType("type-3").setContext("context-3").build())).get(1, TimeUnit.SECONDS);

    int acc = 0;
    boolean matched = false;
    final KafkaConsumer<String, Event> consumer = new KafkaConsumer<>(getConsumerProperties());
    consumer.subscribe(Collections.singletonList(topic));

    while (acc < 3) {
      final ConsumerRecords<String, Event> records = consumer.poll(Duration.ofSeconds(1));
      for (final ConsumerRecord<String, Event> record : records) {
        if ("id-3".equals(record.key())) matched = true;
      }
      // todo: remove crutch ~> use awaitility lib
      acc++;
      Thread.sleep(1000);
    }

    assertTrue(matched);
  }

  private Properties getProducerProperties() {
    final Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
    // wait for acks from all brokers, when replicated [-1, 0, 1]
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    // properties.put(ProducerConfig.RETRIES_CONFIG, 0);
    // tune (increase) throughput
    // properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    // properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    // be sure to use `http://`
    // properties.put(
    //    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
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
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
    // be sure to use `http://`
    // properties.put(
    //    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
    return properties;
  }
}
