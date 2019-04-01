package no.sysco.testing.kafka.streams.topology;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

public class StreamProcessingAvroTest {
  private final String topicIn = "topic-in";
  private final String topicOut = "topic-out";
  private final String schemaUrl = "http://localhost:8081";
  // http://localhost:8081/subjects/topic-in-value/versions/latest
  // only for TopicNameStrategy
  private final String mockedUrl = schemaUrl + "/subjects/" + topicIn + "-value/versions/latest";
  private TopologyTestDriver testDriver;
  private MockSchemaRegistryClient schemaRegistryClient;
  private Properties properties;

  @Before
  public void start() {
    properties = new Properties();
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, "client-id-test-1");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-id-test-5");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9922");
    properties.put(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
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
  public void testTopologyAvro_statelessProcessors() throws IOException, RestClientException {

    /** Arrange */
    // register schema in mock schema-registry
    schemaRegistryClient.register(
        new TopicNameStrategy().subjectName(topicIn, false, Person.SCHEMA$), Person.SCHEMA$);
    // create serde with config to be able to connect to mock schema registry
    final SpecificAvroSerde<Person> serde = new SpecificAvroSerde<>(schemaRegistryClient);

    final Map<String, String> schema = Collections.singletonMap(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "wat-ever-url-anyway-it-is-mocked");
    serde.configure(schema, false);
    // get topology
    final Topology topology =
        StreamProcessingAvro.topologyUpperCase(new Tuple2<>(topicIn, topicOut), serde);
    testDriver = new TopologyTestDriver(topology, properties);

    final ConsumerRecordFactory<String, Person> factory =
        new ConsumerRecordFactory<>(
            topicIn, new StringSerializer(), serde.serializer());

    final ConsumerRecord<byte[], byte[]> inRecord1 = factory.create(
        topicIn,
        "1",
        Person.newBuilder().setId("id-1").setName("nikita").setLastname("zhevnitskiy").build());

    final ConsumerRecord<byte[], byte[]> inRecord2 = factory.create(
        topicIn,
        "2",
        Person.newBuilder().setId("id-2").setName("vitaly").setLastname("moscow").build());

    /** Act */
    testDriver.pipeInput(Arrays.asList(inRecord1, inRecord2));
    final ProducerRecord<String, Person> outRecord1 =
        testDriver.readOutput(topicOut, new StringDeserializer(), serde.deserializer());
    final ProducerRecord<String, Person> outRecord2 =
        testDriver.readOutput(topicOut, new StringDeserializer(), serde.deserializer());

    /** Assert */
    assertEquals("ID-1", outRecord1.value().getId());
    assertEquals("ID-2", outRecord2.value().getId());
    assertEquals("moscow".toUpperCase(), outRecord2.value().getLastname());
  }

  @Test
  public void testTopologyAvro_statefulProcessors() throws IOException, RestClientException {
    /** Arrange */
    final String storeName = "same-name";
    // register schema in mock schema-registry
    schemaRegistryClient.register(
        new TopicNameStrategy().subjectName(topicIn, false, Person.SCHEMA$), Person.SCHEMA$);
    // create serde with config to be able to connect to mock schema registry
    final SpecificAvroSerde<Person> serde = new SpecificAvroSerde<>(schemaRegistryClient);

    final Map<String, String> schema = Collections.singletonMap(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "wat-ever-url-anyway-it-is-mocked");
    serde.configure(schema, false);
    // get topology
    final Topology topology =
        StreamProcessingAvro.topologyCountUsersWithSameName(
            new Tuple2<>(topicIn, topicOut), serde, storeName);
    testDriver = new TopologyTestDriver(topology, properties);

    final ConsumerRecordFactory<String, Person> factory =
        new ConsumerRecordFactory<>(
            topicIn, new StringSerializer(), serde.serializer());

    final ConsumerRecord<byte[], byte[]> inRecord1 = factory.create(
        topicIn,
        "1",
        Person.newBuilder().setId("id-1").setName("nikita").setLastname("zhevnitskiy").build());

    final ConsumerRecord<byte[], byte[]> inRecord2 = factory.create(
        topicIn,
        "2",
        Person.newBuilder().setId("id-2").setName("nikita").setLastname("moscow").build());

    /** Act */
    testDriver.pipeInput(Arrays.asList(inRecord1, inRecord2));
    final KeyValueStore<String, Long> keyValueStore = testDriver.getKeyValueStore(storeName);
    final Long amountOfRecordWithSameName = keyValueStore.get("nikita");

    /** Assert */
    assertEquals(Long.valueOf(2), amountOfRecordWithSameName);
  }
}
