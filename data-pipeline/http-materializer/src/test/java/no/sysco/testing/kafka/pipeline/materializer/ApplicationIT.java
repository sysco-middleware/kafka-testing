package no.sysco.testing.kafka.pipeline.materializer;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import no.sysco.testing.kafka.embedded.EmbeddedSingleNodeKafkaCluster;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class ApplicationIT { //extends WireMockTestBase {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort().dynamicHttpsPort());
  private static final String topic = "topic";

  @Test
  public void ttt() {

  }

}