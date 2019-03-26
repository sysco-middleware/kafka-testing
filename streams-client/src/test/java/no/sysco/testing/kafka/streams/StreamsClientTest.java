package no.sysco.testing.kafka.streams;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class StreamsClientTest {

  private TopologyTestDriver testDriver;
  private Properties properties;
  final String topicIn = "topic-in";
  final String topicOut = "topic-out";

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
  public void testTopology() {
    // Arrange
    final Topology topology = StreamsClient.topologyCountAnagram(new Tuple2<>(topicIn, topicOut));
    // Setup TopologyTestDriver
    testDriver = new TopologyTestDriver(topology, properties);
    final ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(topicIn, new StringSerializer(), new StringSerializer());
    final ConsumerRecord<byte[], byte[]> record1 = factory.create(topicIn, null, "magic");
    final ConsumerRecord<byte[], byte[]> record2 = factory.create(topicIn, null, "cigma");

    // Act
    testDriver.pipeInput(List.of(record1,record2));
    final ProducerRecord<String, Long> outRecord1 = testDriver.readOutput(topicOut, new StringDeserializer(), new LongDeserializer());
    final ProducerRecord<String, Long> outRecord2 = testDriver.readOutput(topicOut, new StringDeserializer(), new LongDeserializer());
    final ProducerRecord<String, Long> outRecord3 = testDriver.readOutput(topicOut, new StringDeserializer(), new LongDeserializer());

    // Assert
    assertNull(outRecord3);
    assertEquals("acgim", outRecord1.key());
    assertEquals("acgim", outRecord2.key());
    assertEquals(Long.valueOf(1), outRecord1.value());
    assertEquals(Long.valueOf(2), outRecord2.value());
  }

}