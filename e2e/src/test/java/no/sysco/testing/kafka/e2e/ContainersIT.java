package no.sysco.testing.kafka.e2e;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import no.sysco.testing.kafka.e2e.representation.MessageJsonRepresentation;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ContainersIT extends ContainersTestBaseIT {


    @Test
    public void is_running() {
        assertTrue(kafka.isRunning());
        assertTrue(schemaRegistry.isRunning());

        assertTrue(jsonServer.isRunning());
        assertTrue(httpProducer.isRunning());
        assertTrue(httpMaterializer.isRunning());

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
    public void test_data_pipeline_flow_successful() {
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
                                            // .statusCode(200)
                                            .extract()
                                            .as(MessageJsonRepresentation.class);

                            assertNotNull(jsonPresentation);
                        });
    }
}
