package no.sysco.testing.kafka.pipeline.producer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.dropwizard.testing.DropwizardTestSupport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import no.sysco.testing.kafka.embedded.EmbeddedSingleNodeKafkaCluster;
import no.sysco.testing.kafka.pipeline.avro.Message;
import no.sysco.testing.kafka.pipeline.producer.domain.MessageJsonRepresentation;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

public class ProducerRestApplicationIT {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String topic = "topic1";
  private static ProducerRestConfig producerRestConfig;
  // https://www.dropwizard.io/1.3.9/docs/manual/testing.html
  private static DropwizardTestSupport<ProducerRestConfig> SUPPORT;

  @BeforeClass
  public static void beforeClass() {
    // init -> service is stateless
    producerRestConfig =
        new ProducerRestConfig(
            "test-config-1",
            new ProducerRestConfig.KafkaClientFactory(
                CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(), topic));
    SUPPORT = new DropwizardTestSupport<>(ProducerRestApplication.class, producerRestConfig);
    SUPPORT.before();
  }

  @AfterClass
  public static void afterClass() {
    SUPPORT.after();
    producerRestConfig = null;
    SUPPORT = null;
  }

  @Test
  public void sendPostRequest_msgProduced_success() {
    Client client = new JerseyClientBuilder().build();

    Response response =
        client
            .target(String.format("http://localhost:%d/messages", SUPPORT.getLocalPort()))
            .request()
            .post(Entity.json(new MessageJsonRepresentation("id-1", "from-1", "to-1", "text-1")));

    assertEquals(202, response.getStatus());

    final KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(getConsumerProperties());
    consumer.subscribe(Collections.singletonList(topic));
    final ArrayList<Message> messages = new ArrayList<>();

    await()
        .atMost(25, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(100));
              for (final ConsumerRecord<String, Message> record : records) {
                messages.add(record.value());
              }
              assertEquals(1, messages.size());
            });
  }

  @Test
  public void sendPostRequest_msgProduced_fail() {
    Client client = new JerseyClientBuilder().build();

    Response response =
        client
            .target(String.format("http://localhost:%d/messages", SUPPORT.getLocalPort()))
            .request()
            // cant construct avro record from this payload
            .post(Entity.json(new MessageJsonRepresentation(null, null, "null", "")));

    // bad request
    assertEquals(400, response.getStatus());

    final KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(getConsumerProperties());
    consumer.subscribe(Collections.singletonList(topic));
    final ArrayList<Message> messages = new ArrayList<>();

    await()
        .pollDelay(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final ConsumerRecords<String, Message> records = consumer.poll(Duration.ofSeconds(1));
              for (final ConsumerRecord<String, Message> record : records) {
                messages.add(record.value());
              }
              assertEquals(0, messages.size());
            });
  }

  private Properties getConsumerProperties() {
    final Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    properties.put(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-"+ UUID.randomUUID().toString());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "gr-" + UUID.randomUUID().toString());
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return properties;
  }
}
