package no.sysco.testing.kafka.pipeline.materializer;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import no.sysco.testing.kafka.embedded.EmbeddedSingleNodeKafkaCluster;
import no.sysco.testing.kafka.pipeline.avro.Message;
import no.sysco.testing.kafka.pipeline.materializer.infrastructure.service.DatabaseWebService;
import okhttp3.MediaType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class ApplicationIT {

  private static final String JSON = "application/json; charset=utf-8";

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

  @Before
  public void setup() {

  }

  @Test
  public void testApplication_InMemory_3Msg_success()
      throws ExecutionException, InterruptedException {

    String topic = "topic1";
    CLUSTER.createTopic(topic);

    // Arrange
      // config
    final String dbRestServiceUrl = "http://whatever:1234/messages";
    final MaterializerConfig.KafkaConfig kafkaConfig =
        new MaterializerConfig.KafkaConfig(CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(),
            topic);
    final MaterializerConfig.DatabaseRestServiceConfig dbRestServiceConfig =
        new MaterializerConfig.DatabaseRestServiceConfig(dbRestServiceUrl);
    final MaterializerConfig testConfig =
        new MaterializerConfig("test", kafkaConfig, dbRestServiceConfig);
    final MaterializerApplication materializerApplication =
        new MaterializerApplication(testConfig, true);

      // send 3 records
    final KafkaProducer<String, Message> messageProducer =
        TestUtils.getMessageProducer(kafkaConfig);
    for (int i = 1; i<4; i++) {
      final Message msg =
          Message.newBuilder().setId(i+"").setFrom("from-"+i).setTo("to-"+i).setText("text-"+i).build();
      messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg))
          .get(); // blocking
    }


    // Act
    final DatabaseWebService dbWebService = materializerApplication.getDbWebService();
      // verify that
    assertEquals(0, dbWebService.getMessages().size());
      // start app
    materializerApplication.start();

    // Assert
    await().atMost(15, TimeUnit.SECONDS)
        .until(() -> dbWebService.getMessages().size() == 3);

    // stop stream app
    materializerApplication.stop();
  }

  @Test
  public void testApplication_3Msg_success() throws ExecutionException, InterruptedException {
    String topic = "topic2";
    CLUSTER.createTopic(topic);

    // Arrange
      // config
    final String baseUrl = wireMockRule.baseUrl();
    final String dbRestServiceUrl = baseUrl+"/messages";
    final MaterializerConfig.KafkaConfig kafkaConfig =
        new MaterializerConfig.KafkaConfig(CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(),
            topic);
    final MaterializerConfig.DatabaseRestServiceConfig dbRestServiceConfig =
        new MaterializerConfig.DatabaseRestServiceConfig(dbRestServiceUrl);
    final MaterializerConfig testConfig =
        new MaterializerConfig("test", kafkaConfig, dbRestServiceConfig);

    final MaterializerApplication materializerApplication =
        new MaterializerApplication(testConfig, false);

      // wiremock stub
    stubFor(post(urlEqualTo("/messages"))
        //.withHeader("Accept", equalTo(JSON))
        //.withHeader("Content-Type", equalTo(JSON))
        .willReturn(aResponse()
            .withStatus(201)
            .withHeader("Content-Type", JSON)));

    // send 3 records
    final KafkaProducer<String, Message> messageProducer =
        TestUtils.getMessageProducer(kafkaConfig);
    for (int i = 1; i<4; i++) {
      final Message msg =
          Message.newBuilder().setId(i+"").setFrom("from-"+i).setTo("to-"+i).setText("text-"+i).build();
      messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg))
          .get(); // blocking
    }

    // Act
    materializerApplication.start();

    // Assert
    await().atMost(15, TimeUnit.SECONDS).untilAsserted(()->
      // and verify 3 request happen
      verify(3, postRequestedFor(urlEqualTo("/messages")))
    );

    materializerApplication.stop();
  }

}