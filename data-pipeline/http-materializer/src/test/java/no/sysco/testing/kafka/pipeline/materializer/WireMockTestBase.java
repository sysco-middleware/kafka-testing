package no.sysco.testing.kafka.pipeline.materializer;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.Before;
import org.junit.BeforeClass;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

abstract class WireMockTestBase {

  static WireMockServer wireMockServer;

  @BeforeClass
  public static void startWireMock() {
    wireMockServer = new WireMockServer(wireMockConfig().dynamicPort().dynamicHttpsPort()); //No-args constructor will start on port 8080, no HTTPS
    wireMockServer.start();
  }

  @Before
  public void reset() {
    wireMockServer.resetAll();
  }

}
