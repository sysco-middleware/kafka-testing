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

    private String JSON_SERVER_URL;
    private String HTTP_PRODUCER_BASE_URL;

  /**
   * Environment container contains composition of containers which are declared
   * in docker-compose.test.yml file. Use a local Docker Compose binary.
   * Waiting strategies are applied to `service-name` with suffix `_1`
   */
  @ClassRule
  public static DockerComposeContainer environment =
      new DockerComposeContainer(new File("docker-compose.test.yml"))
          .withLocalCompose(true)
          .waitingFor("db-mock_1", Wait.forHttp("/").forStatusCode(200))
          .waitingFor("schema-registry_1", Wait.forHttp("/subjects").forStatusCode(200))
          .waitingFor("http-producer_1", Wait.forHttp("/messages").forStatusCode(200))
          .withExposedService("db-mock_1", 80)
          .withExposedService("http-producer_1", 8080);

    @Test
    public void is_running() {
//        System.out.println("HERE: "+environment.getServiceHost("db-mock_1", 80)+":"+environment.getServicePort("db-mock_1", 80));
        JSON_SERVER_URL = "http://" + environment.getServiceHost("db-mock_1", 80) + ":" + environment.getServicePort("db-mock_1", 80);
        final List<MessageJsonRepresentation> messageJsonRepresentations =
                Arrays.asList(
                        RestAssured.given()
                                .get(JSON_SERVER_URL + "/messages")
                                .then()
                                .statusCode(200)
                                .extract()
                                .as(MessageJsonRepresentation[].class));
        assertTrue(messageJsonRepresentations.size() > 0);
    }

    @Test
    public void test_data_pipeline_flow_successful() {
        JSON_SERVER_URL = "http://" + environment.getServiceHost("db-mock_1", 80) + ":" + environment.getServicePort("db-mock_1", 80);
        HTTP_PRODUCER_BASE_URL = "http://" + environment.getServiceHost("http-producer_1", 8080) + ":" + environment.getServicePort("http-producer_1", 8080);

        String id = UUID.randomUUID().toString();
        String from = UUID.randomUUID().toString();
        String to = UUID.randomUUID().toString();
        String text = UUID.randomUUID().toString();

        MessageJsonRepresentation messageJsonRepresentation =
                new MessageJsonRepresentation(id, from, to, text);

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body(messageJsonRepresentation)
                .post(HTTP_PRODUCER_BASE_URL + "/messages")
                .then()
                .statusCode(202);

        await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            MessageJsonRepresentation jsonRepresentation =
                                    RestAssured.given()
                                            .get(JSON_SERVER_URL + "/messages/" + id)
                                            .then()
                                            .extract()
                                            .as(MessageJsonRepresentation.class);

                            assertNotNull(jsonRepresentation);
                        });
    }
}
