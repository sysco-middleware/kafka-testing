package no.sysco.testing.kafka.streams.topology;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import no.sysco.testing.kafka.streams.avro.Person;
import no.sysco.testing.kafka.streams.utils.Tuple2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class StreamProcessingAvroTest {
  private TopologyTestDriver testDriver;
  private Properties properties;
  private final String topicIn    = "topic-in";
  private final String topicOut   = "topic-out";
  private final String schemaUrl  = "http://localhost:8081";
  // http://localhost:8081/subjects/topic-in-value/versions/latest
  // only for TopicNameStrategy
  private final String mockedUrl = schemaUrl + "/subjects/" + topicIn + "-value/versions/latest";


  @Before
  public void start() {
    properties = new Properties();
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, "client-id-test-1");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-id-test-5");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9922");
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
  }

  @After
  public void tearDown() {
    Optional.ofNullable(testDriver).ifPresent(TopologyTestDriver::close);
    testDriver = null;
    properties = null;
  }

  @Test
  public void testTopologyAvro_statelessProcessors_Uppercase_approach1()
      throws IOException, RestClientException {

    // Arrange
    MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    schemaRegistryClient.register(new TopicNameStrategy().subjectName(topicIn, false, Person.SCHEMA$), Person.SCHEMA$);
    schemaRegistryClient.register(new TopicNameStrategy().subjectName(topicIn, true, Person.SCHEMA$), Person.SCHEMA$);

    final Collection<String> allSubjects = schemaRegistryClient.getAllSubjects();
    System.out.println("HERE: "+allSubjects);
    System.out.println("HERE: "+schemaRegistryClient.getLatestSchemaMetadata("topic-in-value"));
    System.out.println("HERE: "+schemaRegistryClient.getBySubjectAndId("topic-in-value", 1));

    SpecificAvroSerde<Person> serde = new SpecificAvroSerde<>(schemaRegistryClient);

    final Topology topology = StreamProcessingAvro.topologyUpperCase(new Tuple2<>(topicIn, topicOut), schemaUrl, schemaRegistryClient);

    properties.put("schema.registry.url", "http://localhost:8081");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

    testDriver = new TopologyTestDriver(topology, properties);

    //final SpecificAvroSerializer<Person> personSpecificAvroSerializer =
    //    new SpecificAvroSerializer<>();
    //final var schema = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
    //personSpecificAvroSerializer.configure(schema, false);
    final ConsumerRecordFactory<String, Person> factory =
        new ConsumerRecordFactory<>(topicIn, new StringSerializer(), serde.serializer());
    final ConsumerRecord<byte[], byte[]> record1 = factory.create(topicIn, "1", Person.newBuilder()
        .setId("id-1")
        .setName("nikita")
        .setLastname("zhevnitskiy")
        .build()
    );

    testDriver.pipeInput(List.of(record1));

  }

  //@Test
  //public void testTopologyAvro_statelessProcessors_Uppercase_approach2()
  //    throws IOException, RestClientException {
  //  // Arrange
  //  MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
  //  schemaRegistryClient.register("topic-in-value", Person.SCHEMA$);
  //  schemaRegistryClient.register("topic-in-key", Person.SCHEMA$);
  //
  //  SpecificAvroSerde<Person> serde = new SpecificAvroSerde<>(schemaRegistryClient);
  //
  //  final Topology topology = StreamProcessingAvro.topologyUpperCase(new Tuple2<>(topicIn, topicOut), schemaRegistryClient);
  //  testDriver = new TopologyTestDriver(topology, properties);
  //
  //
  //  final SpecificAvroSerializer<Person> personSpecificAvroSerializer = new SpecificAvroSerializer<>();
  //  final var schema = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://dummy:1234");
  //  personSpecificAvroSerializer.configure(schema, false);
  //  final ConsumerRecordFactory<String, Person> factory =
  //      new ConsumerRecordFactory<>(topicIn, new StringSerializer(), personSpecificAvroSerializer);
  //
  //
  //  final ConsumerRecord<byte[], byte[]> record1 = factory.create(topicIn, "1", Person.newBuilder()
  //      .setId("id-1")
  //      .setName("nikita")
  //      .setLastname("zhevnitskiy")
  //      .build()
  //  );
  //
  //}

  //@Test
  //public void serializationIssue() throws IOException, RestClientException {
  //  //MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
  //  //final int register = mockSchemaRegistryClient.register("topic-in-value", Person.SCHEMA$);
  //  //System.out.println("REGISTERED: "+register);
  //  //final Topology topology = StreamProcessingAvro.topologyUpperCase(new Tuple2<>(topicIn, topicOut), mockSchemaRegistryClient);
  //  //testDriver = new TopologyTestDriver(topology, properties);
  //
  //  final SpecificAvroSerializer<Person> personSpecificAvroSerializer = new SpecificAvroSerializer<>();
  //  final var schema = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
  //
  //  personSpecificAvroSerializer.configure(schema, false);
  //
  //  final ConsumerRecordFactory<String, Person> factory =
  //      new ConsumerRecordFactory<>(topicIn, new StringSerializer(), personSpecificAvroSerializer);
  //
  //  final Person person = Person.newBuilder()
  //      .setId("id-1")
  //      .setName("nikita")
  //      .setLastname("zhevnitskiy")
  //      .build();
  //
  //  final ConsumerRecord<byte[], byte[]> record = factory.create(topicIn, "1", person);
  //
  //}

}