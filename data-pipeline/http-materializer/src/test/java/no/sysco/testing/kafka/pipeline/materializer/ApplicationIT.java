package no.sysco.testing.kafka.pipeline.materializer;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import no.sysco.testing.kafka.embedded.EmbeddedSingleNodeKafkaCluster;
import no.sysco.testing.kafka.pipeline.avro.Message;
import no.sysco.testing.kafka.pipeline.materializer.infrastructure.service.DatabaseWebService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ApplicationIT {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String JSON = "application/json; charset=utf-8";
  @Rule public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

  @Before
  public void setup() {}

  @Test
  public void send_InMemory_3Msg_success() throws ExecutionException, InterruptedException {

    String topic = "topic1";
    CLUSTER.createTopic(topic);

    // Arrange
    // config
    final String dbRestServiceUrl = "http://whatever:1234/messages";
    final MaterializerConfig.KafkaConfig kafkaConfig =
        new MaterializerConfig.KafkaConfig(
            CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(), topic);
    final MaterializerConfig.DatabaseRestServiceConfig dbRestServiceConfig =
        new MaterializerConfig.DatabaseRestServiceConfig(dbRestServiceUrl);
    final MaterializerConfig testConfig =
        new MaterializerConfig("test", kafkaConfig, dbRestServiceConfig);
    final MaterializerApplication materializerApplication =
        new MaterializerApplication(testConfig, true);

    // send 3 records
    final KafkaProducer<String, Message> messageProducer =
        TestUtils.getMessageProducer(kafkaConfig);
    for (int i = 1; i < 4; i++) {
      final Message msg =
          Message.newBuilder()
              .setId(i + "")
              .setFrom("from-" + i)
              .setTo("to-" + i)
              .setText("text-" + i)
              .build();
      messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg)).get(); // blocking
    }

    // Act
    final DatabaseWebService dbWebService = materializerApplication.getDbWebService();
    // verify that
    assertEquals(0, dbWebService.getMessages().size());
    // start app
    materializerApplication.start();

    // Assert
    await().atMost(15, TimeUnit.SECONDS).until(() -> dbWebService.getMessages().size() == 3);

    // stop stream app
    materializerApplication.stop();
  }

  @Test
  public void send_3Msg_success() throws ExecutionException, InterruptedException {
    String topic = "topic2";
    CLUSTER.createTopic(topic);

    // Arrange
    // config
    final String baseUrl = wireMockRule.baseUrl();
    final String dbRestServiceUrl = baseUrl + "/messages";
    final MaterializerConfig.KafkaConfig kafkaConfig =
        new MaterializerConfig.KafkaConfig(
            CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(), topic);
    final MaterializerConfig.DatabaseRestServiceConfig dbRestServiceConfig =
        new MaterializerConfig.DatabaseRestServiceConfig(dbRestServiceUrl);
    final MaterializerConfig testConfig =
        new MaterializerConfig("test", kafkaConfig, dbRestServiceConfig);

    final MaterializerApplication materializerApplication =
        new MaterializerApplication(testConfig, false);

    // wiremock stub
    stubFor(
        post(urlEqualTo("/messages"))
            // .withHeader("Accept", equalTo(JSON))
            // .withHeader("Content-Type", equalTo(JSON))
            .willReturn(aResponse().withStatus(201).withHeader("Content-Type", JSON)));

    // send 3 records
    final KafkaProducer<String, Message> messageProducer =
        TestUtils.getMessageProducer(kafkaConfig);
    for (int i = 1; i < 4; i++) {
      final Message msg =
          Message.newBuilder()
              .setId(i + "")
              .setFrom("from-" + i)
              .setTo("to-" + i)
              .setText("text-" + i)
              .build();
      messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg)).get(); // blocking
    }

    // Act
    materializerApplication.start();

    // Assert
    await()
        .atMost(15, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                // verify 3 POST requests happen
                verify(3, postRequestedFor(urlEqualTo("/messages"))));

    materializerApplication.stop();
  }

  @Test
  public void send_1Msg_3times_failAfter_2nd() throws ExecutionException, InterruptedException {
    String topic = "topic3";
    CLUSTER.createTopic(topic);

    // Arrange
    // config
    final String baseUrl = wireMockRule.baseUrl();
    final String dbRestServiceUrl = baseUrl + "/messages";
    final MaterializerConfig.KafkaConfig kafkaConfig =
        new MaterializerConfig.KafkaConfig(
            CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(), topic);
    final MaterializerConfig.DatabaseRestServiceConfig dbRestServiceConfig =
        new MaterializerConfig.DatabaseRestServiceConfig(dbRestServiceUrl);
    final MaterializerConfig testConfig =
        new MaterializerConfig("test", kafkaConfig, dbRestServiceConfig);

    final MaterializerApplication materializerApplication =
        new MaterializerApplication(testConfig, false);

    // start app
    materializerApplication.start();
    final KafkaStreams.State runningState =
        materializerApplication.getKafkaMessageMaterializer().getState();

    assertTrue(runningState.isRunning());

    // init producer
    final KafkaProducer<String, Message> messageProducer =
        TestUtils.getMessageProducer(kafkaConfig);
    final Message msg =
        Message.newBuilder()
            .setId("id-1")
            .setFrom("from-1")
            .setTo("to-1")
            .setText("text-1")
            .build();

    // wiremock stub
    stubFor(
        post(urlEqualTo("/messages"))
            .willReturn(aResponse().withStatus(201).withHeader("Content-Type", JSON)));
    // send record
    messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg)).get(); // blocking

    await()
        .atMost(15, TimeUnit.SECONDS)
        .untilAsserted(() -> verify(1, postRequestedFor(urlEqualTo("/messages"))));

    stubFor(
        post(urlEqualTo("/messages"))
            .willReturn(
                aResponse()
                    // conflict, entity with id already exist
                    .withStatus(409)
                    .withHeader("Content-Type", JSON)));

    // send record again 2nd time
    messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg)).get(); // blocking
    // send record again 3rd time
    messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg)).get();

    await()
        .atMost(15, TimeUnit.SECONDS)
        // ignore exceptions, otherwise test will fail
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              System.out.println(
                  "STATE: "
                      + materializerApplication
                          .getKafkaMessageMaterializer()
                          .getState()
                          .isRunning());
              // assert that streams is not running due to exception
              assertFalse(
                  materializerApplication.getKafkaMessageMaterializer().getState().isRunning());
            });

    // 3 msg produced but stream failed and stopped after 2nd msg was processed -> only 2 request
    // were made
    verify(2, postRequestedFor(urlEqualTo("/messages")));

    materializerApplication.stop();
  }
}
