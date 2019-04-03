package no.sysco.testing.kafka.pipeline.producer;

import io.dropwizard.setup.Environment;
import java.util.logging.Logger;

public class Application extends io.dropwizard.Application<ApplicationConfig> {

  private static Logger log = Logger.getLogger(Application.class.getName());

  public static void main(String[] args) throws Exception {
    new Application().run(args);
  }



  @Override public void run(ApplicationConfig applicationConfig, Environment environment)
      throws Exception {

  }
}
