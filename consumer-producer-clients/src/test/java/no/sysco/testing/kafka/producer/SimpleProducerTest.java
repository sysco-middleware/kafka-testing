package no.sysco.testing.kafka.producer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import no.sysco.testing.kafka.embedded.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SimpleProducerTest {

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
  public void testSimpleProducer()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());

    // async with callback
    producer.send(
        new ProducerRecord<>(topic, "k1", "v1"),
        ((metadata, exception) -> {
          if (exception == null) {
            System.out.println("SUCCESS bro");
          } else {
            fail();
          }
        }));

    // sync
    final RecordMetadata recordMetadata =
        producer.send(new ProducerRecord<>(topic, "k2", "v2")).get(3, TimeUnit.SECONDS);

    assertTrue(recordMetadata.hasOffset());
    assertTrue(recordMetadata.hasTimestamp());
  }

  private Properties getProperties() {
    final Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    // wait for acks from all brokers, when replicated [-1, 0, 1]
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.RETRIES_CONFIG, 0);
    // tune (increase) throughput
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    // be sure to use `http://`
    properties.put(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
    return properties;
  }
}
