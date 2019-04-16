package no.sysco.testing.kafka.streams.topology;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import no.sysco.testing.kafka.streams.avro.Person;
import no.sysco.testing.kafka.streams.utils.Tuple2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StreamProcessingLowLvlAvroTest {
  private final String topicIn = "topic-in";
  private final String topicOut = "topic-out";
  private TopologyTestDriver testDriver;
  private MockSchemaRegistryClient schemaRegistryClient;
  private Properties properties;

  @Before
  public void start() {
    properties = new Properties();
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, "client-id-test-1");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-id-test-5");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9922");
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://whatever:4242");
    properties.put(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

    schemaRegistryClient = new MockSchemaRegistryClient();
  }

  @After
  public void tearDown() {
    Optional.ofNullable(testDriver).ifPresent(TopologyTestDriver::close);
    testDriver = null;
    properties = null;
  }

  @Test
  public void testTopologyLowLvlAvro_statefulProcessors() throws IOException, RestClientException {
    /** Arrange */
    final String storeName = "same-name";

    // create serde with config to be able to connect to mock schema registry
    final SpecificAvroSerde<Person> serde = new SpecificAvroSerde<>(schemaRegistryClient);

    final Map<String, String> schema =
        Collections.singletonMap(
            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "wat-ever-url-anyway-it-is-mocked");
    serde.configure(schema, false);
    // get topology
    final Topology topology =
        StreamProcessingLowLvlAvro.topologyDedupByUserId(
            new Tuple2<>(topicIn, topicOut), serde, storeName);
    testDriver = new TopologyTestDriver(topology, properties);

    final ConsumerRecordFactory<String, Person> factory =
        new ConsumerRecordFactory<>(topicIn, new StringSerializer(), serde.serializer());

    final ConsumerRecord<byte[], byte[]> inRecord1 =
        factory.create(
            topicIn,
            "id-1",
            Person.newBuilder().setId("id-1").setName("nikita").setLastname("zhevnitskiy").build());

    /** Act */
    testDriver.pipeInput(Collections.singletonList(inRecord1));
    final KeyValueStore<String, Person> dedupStore = testDriver.getKeyValueStore(storeName);
    final Person person = dedupStore.get("id-1");
    final long l = dedupStore.approximateNumEntries();
    System.out.println("Here : " + l);

    /** Assert */
    assertEquals("id-1", person.getId());
  }

  @Test
  public void testTopologyLowLvlAvro_statefulProcessors_invalidInput()
      throws IOException, RestClientException {
    /** Arrange */
    final String storeName = "same-name";

    // create serde with config to be able to connect to mock schema registry
    final SpecificAvroSerde<Person> serde = new SpecificAvroSerde<>(schemaRegistryClient);

    final Map<String, String> schema =
        Collections.singletonMap(
            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "wat-ever-url-anyway-it-is-mocked");
    serde.configure(schema, false);
    // get topology
    final Topology topology =
        StreamProcessingLowLvlAvro.topologyDedupByUserId(
            new Tuple2<>(topicIn, topicOut), serde, storeName);
    testDriver = new TopologyTestDriver(topology, properties);

    final ConsumerRecordFactory<String, Person> factory =
        new ConsumerRecordFactory<>(topicIn, new StringSerializer(), serde.serializer());

    final ConsumerRecord<byte[], byte[]> inRecord1 =
        factory.create(
            topicIn,
            "invalid-id",
            Person.newBuilder().setId("id-1").setName("yohoho").setLastname("pirate").build());

    final ConsumerRecord<byte[], byte[]> inRecord2 =
        factory.create(
            topicIn,
            "id-1",
            Person.newBuilder().setId("id-1").setName("nikita").setLastname("zhevnitskiy").build());

    final ConsumerRecord<byte[], byte[]> inRecord3 =
        factory.create(
            topicIn,
            "id-1",
            Person.newBuilder().setId("id-1").setName("42").setLastname("42").build());

    /** Act */
    testDriver.pipeInput(Arrays.asList(inRecord1, inRecord2, inRecord3));
    final KeyValueStore<String, Person> dedupStore = testDriver.getKeyValueStore(storeName);
    final Person person = dedupStore.get("id-1");

    final ProducerRecord<String, Person> outRecord1 =
        testDriver.readOutput(topicOut, new StringDeserializer(), serde.deserializer());
    final ProducerRecord<String, Person> outRecord2 =
        testDriver.readOutput(topicOut, new StringDeserializer(), serde.deserializer());
    final ProducerRecord<String, Person> outRecord3 =
        testDriver.readOutput(topicOut, new StringDeserializer(), serde.deserializer());

    /** Assert */
    assertEquals("nikita", outRecord1.value().getName());
    assertEquals("id-1", outRecord1.key());
    assertEquals("id-1", person.getId());
    assertNull(outRecord2);
    assertNull(outRecord3);

    assertEquals("zhevnitskiy", dedupStore.get("id-1").getLastname());
    assertNull(dedupStore.get("invalid-id"));

    assertEquals(1, dedupStore.approximateNumEntries());
  }
}
