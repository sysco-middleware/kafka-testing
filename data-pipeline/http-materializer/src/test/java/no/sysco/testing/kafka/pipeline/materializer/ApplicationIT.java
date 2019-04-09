package no.sysco.testing.kafka.pipeline.materializer;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import no.sysco.testing.kafka.embedded.EmbeddedSingleNodeKafkaCluster;
import org.junit.ClassRule;
import org.junit.Rule;

import static org.junit.Assert.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class ApplicationIT {
  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(8089); // No-args constructor defaults to port 8080

  private static final String topic = "topic";

}