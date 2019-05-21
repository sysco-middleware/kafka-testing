package no.sysco.testing.kafka.e2e;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import no.sysco.testing.kafka.e2e.representation.MessageJsonRepresentation;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ContainersIT {

    public static final String LOCAL_HOST = "http://localhost";
    public static final String TOPIC = "events-message-v1";
    public static final String CONFLUENT_PLATFORM_VERSION = "5.1.1";

    // kafka env
    public static KafkaContainer kafka;
    public static String KAFKA_BROKER_INSIDE_DOCKER_ENV;
    public static GenericContainer schemaRegistry;
    public static String SCHEMA_REGISTRY_INSIDE_DOCKER_ENV;

    // app env
    public static GenericContainer jsonServer;
    public static String JSON_SERVER_INSIDE_DOCKER_ENV;
    public static String JSON_SERVER_EXPOSED;

    public static GenericContainer httpProducer;
    public static String HTTP_PRODUCER_EXPOSED;

    public static GenericContainer httpMaterializer;

    @BeforeClass
    public static void start() {
        setZookeeperAndKafka();
        setSchemaRegistry();

        setJsonServer();
        setHttProducer();
        setHttpMaterializer();
    }

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

    private static void setZookeeperAndKafka() {
        kafka = new KafkaContainer(CONFLUENT_PLATFORM_VERSION);
        kafka.start();
    }

    private static void setSchemaRegistry() {
        // get network alias to be able connect other container to env
        KAFKA_BROKER_INSIDE_DOCKER_ENV = "PLAINTEXT://" + kafka.getNetworkAliases().get(0) + ":9092";
        schemaRegistry =
                new GenericContainer("confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION)
                        .withExposedPorts(8081)
                        .withNetwork(kafka.getNetwork())
                        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
                        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", KAFKA_BROKER_INSIDE_DOCKER_ENV)
                        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
        schemaRegistry.start();
        SCHEMA_REGISTRY_INSIDE_DOCKER_ENV =
                "http://" + schemaRegistry.getNetworkAliases().get(0) + ":8081";
    }

    private static void setJsonServer() {
        jsonServer =
                new GenericContainer("zhenik/json-server")
                        .withExposedPorts(80)
                        // all containers put in same network
                        .withNetwork(kafka.getNetwork())
                        .withEnv("ID_MAP", "id")
                        .withNetworkAliases("json-server")
                        .withClasspathResourceMapping(
                                "json-server-database.json", "/data/db.json", BindMode.READ_WRITE)
                        .waitingFor(Wait.forHttp("/").forStatusCode(200));

        jsonServer.start();
        // provide availability make http calls from localhost against docker env
        JSON_SERVER_EXPOSED = LOCAL_HOST + ":" + jsonServer.getMappedPort(80);
        JSON_SERVER_INSIDE_DOCKER_ENV = "http://" + jsonServer.getNetworkAliases().get(0) + ":80";
    }

    private static void setHttProducer() {
        httpProducer =
                new GenericContainer("zhenik/http-producer:data-pipeline")
                        .withExposedPorts(8080)
                        .withNetwork(kafka.getNetwork())
                        .withEnv("APPLICATION_PORT", "8080")
                        .withEnv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BROKER_INSIDE_DOCKER_ENV)
                        .withEnv("SCHEMA_REGISTRY_URL", SCHEMA_REGISTRY_INSIDE_DOCKER_ENV)
                        .withEnv("SINK_TOPIC", TOPIC)
                        .waitingFor(Wait.forHttp("/messages").forStatusCode(200));
        httpProducer.start();

        HTTP_PRODUCER_EXPOSED = LOCAL_HOST + ":" + httpProducer.getMappedPort(8080);
    }

    private static void setHttpMaterializer() {

        createTopic(TOPIC);

        String messageEndpoint = "http://json-server/messages";
        System.out.println("HERE: " + messageEndpoint);
        httpMaterializer =
                new GenericContainer("zhenik/http-materializer:data-pipeline")
                        .withNetwork(kafka.getNetwork())
                        .withEnv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BROKER_INSIDE_DOCKER_ENV)
                        .withEnv("SCHEMA_REGISTRY_URL", SCHEMA_REGISTRY_INSIDE_DOCKER_ENV)
                        .withEnv("DATABASE_REST_SERVICE_URL", messageEndpoint)
                        .withEnv("SOURCE_TOPIC", TOPIC);
        httpMaterializer.start();
    }

    private static void createTopic(String topicName) {
        // kafka container uses with embedded zookeeper
        // confluent platform and Kafka compatibility 5.1.x <-> kafka 2.1.x
        // kafka 2.1.x require option --zookeeper, later versions use --bootstrap-servers instead
        String createTopic =
                String.format(
                        "/usr/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic %s",
                        topicName);
        try {
            final Container.ExecResult execResult = kafka.execInContainer("/bin/sh", "-c", createTopic);
            if (execResult.getExitCode() != 0) fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
}
