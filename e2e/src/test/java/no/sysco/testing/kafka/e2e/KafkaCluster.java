package no.sysco.testing.kafka.e2e;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import static org.junit.Assert.assertTrue;

public class KafkaCluster {

  @ClassRule public static KafkaContainer kafka;
  @ClassRule public static GenericContainer schemaRegistry;
  public static final String confluentPlatformVersion = "5.1.1";

  @BeforeClass
  public static void start() {
    kafka = new KafkaContainer(confluentPlatformVersion);
    kafka.start();

    // get network alias to be able connect other container to env
    String kafkaInsideDocker = "PLAINTEXT://"+kafka.getNetworkAliases().get(0)+":9092";

    schemaRegistry =
        new GenericContainer(
            "confluentinc/cp-schema-registry:" + confluentPlatformVersion)
            .withExposedPorts(8081)
            .withNetwork(kafka.getNetwork())
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaInsideDocker)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    schemaRegistry.start();
  }

  @Test
  public void isRunning() {
    assertTrue(schemaRegistry.isRunning());
  }
}
