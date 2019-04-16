package no.sysco.testing.kafka.streams.topology;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import no.sysco.testing.kafka.streams.utils.Tuple2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
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

public class StreamProcessingTest {
  private final String topicIn = "topic-in";
  private final String topicOut = "topic-out";
  private TopologyTestDriver testDriver;
  private Properties properties;

  @Before
  public void start() {
    properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
  }

  @After
  public void tearDown() {
    Optional.ofNullable(testDriver).ifPresent(TopologyTestDriver::close);
    testDriver = null;
    properties = null;
  }

  @Test
  public void testTopology_statelessProcessors_Uppercase() {
    // Arrange
    final Topology topology = StreamProcessing.topologyUpperCase(new Tuple2<>(topicIn, topicOut));
    testDriver = new TopologyTestDriver(topology, properties);
    final ConsumerRecordFactory<String, String> factory =
        new ConsumerRecordFactory<>(topicIn, new StringSerializer(), new StringSerializer());
    final ConsumerRecord<byte[], byte[]> record1 = factory.create(topicIn, null, "magic");
    final ConsumerRecord<byte[], byte[]> record2 = factory.create(topicIn, null, "cigma");

    // Act
    testDriver.pipeInput(Arrays.asList(record1, record2));
    final ProducerRecord<String, String> outRecord1 =
        testDriver.readOutput(topicOut, new StringDeserializer(), new StringDeserializer());
    final ProducerRecord<String, String> outRecord2 =
        testDriver.readOutput(topicOut, new StringDeserializer(), new StringDeserializer());
    final ProducerRecord<String, String> outRecord3 =
        testDriver.readOutput(topicOut, new StringDeserializer(), new StringDeserializer());

    // Assert
    assertNull(outRecord3);
    assertEquals("magic".toUpperCase(), outRecord1.value());
    assertEquals("cigma".toUpperCase(), outRecord2.value());
  }

  @Test
  public void testTopology_statefullProcessors_Anagram() {

    // Arrange
    final String storeName = "count-storage";
    final Topology topology =
        StreamProcessing.topologyCountAnagram(new Tuple2<>(topicIn, topicOut), storeName);
    // setup TopologyTestDriver
    testDriver = new TopologyTestDriver(topology, properties);
    final ConsumerRecordFactory<String, String> factory =
        new ConsumerRecordFactory<>(topicIn, new StringSerializer(), new StringSerializer());
    final ConsumerRecord<byte[], byte[]> record1 = factory.create(topicIn, null, "magic");
    final ConsumerRecord<byte[], byte[]> record2 = factory.create(topicIn, null, "cigma");

    // Act
    testDriver.pipeInput(Arrays.asList(record1, record2));
    final ProducerRecord<String, Long> outRecord1 =
        testDriver.readOutput(topicOut, new StringDeserializer(), new LongDeserializer());
    final ProducerRecord<String, Long> outRecord2 =
        testDriver.readOutput(topicOut, new StringDeserializer(), new LongDeserializer());
    final ProducerRecord<String, Long> outRecord3 =
        testDriver.readOutput(topicOut, new StringDeserializer(), new LongDeserializer());

    // accessing storage
    final KeyValueStore<String, Long> keyValueStore = testDriver.getKeyValueStore(storeName);
    final Long amountOfRecordsInStorageByKey = keyValueStore.get("acgim");

    // Assert
    assertNull(outRecord3);
    assertEquals("acgim", outRecord1.key());
    assertEquals("acgim", outRecord2.key());
    assertEquals(Long.valueOf(1), outRecord1.value());
    assertEquals(Long.valueOf(2), outRecord2.value());
    assertEquals(Long.valueOf(2), amountOfRecordsInStorageByKey);
  }
}
