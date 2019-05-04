package no.sysco.testing.kafka.e2e;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import no.sysco.testing.kafka.e2e.representation.MessageJsonRepresentation;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DockerComposeIT {

  public static final String TOPIC = "events-message-v1";
  public static final String JSON_SERVER_EXPOSED = "http://localhost:3000";
  public static final String HTTP_PRODUCER_EXPOSED = "http://localhost:8080";

  @ClassRule
  public static DockerComposeContainer environment =
      new DockerComposeContainer(new File("docker-compose.test.yml"))
          .withLocalCompose(true)
          .waitingFor("db-mock_1", Wait.forHttp("/").forStatusCode(200))
          .waitingFor("schema-registry_1", Wait.forHttp("/subjects").forStatusCode(200))
          .waitingFor("http-producer_1", Wait.forHttp("/messages").forStatusCode(200));

  @Test
  public void isRunning() {
    final List<MessageJsonRepresentation> messageJsonRepresentations =
        Arrays.asList(
            RestAssured.given()
                .get(JSON_SERVER_EXPOSED + "/messages")
                .then()
                .statusCode(200)
                .extract()
                .as(MessageJsonRepresentation[].class));
    assertTrue(messageJsonRepresentations.size() > 0);
  }

  @Test
  public void data_pipeline_flow_success() {
    String id = UUID.randomUUID().toString();
    String from = UUID.randomUUID().toString();
    String to = UUID.randomUUID().toString();
    String text = UUID.randomUUID().toString();

    MessageJsonRepresentation messageJsonRepresentation =
        new MessageJsonRepresentation(id, from, to, text);

    RestAssured.given()
        .contentType(ContentType.JSON)
        .body(messageJsonRepresentation)
        .post(HTTP_PRODUCER_EXPOSED + "/messages")
        .then()
        .statusCode(202);

    await()
        .atMost(70, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              MessageJsonRepresentation jsonPresentation =
                  RestAssured.given()
                      .get(JSON_SERVER_EXPOSED + "/messages/" + id)
                      .then()
                      .extract()
                      .as(MessageJsonRepresentation.class);

              assertNotNull(jsonPresentation);
            });
  }
}
