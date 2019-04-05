package no.sysco.testing.kafka.pipeline.materializer;

import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.logging.Logger;


public class Application extends io.dropwizard.Application<ApplicationConfig> {

  private static Logger log = Logger.getLogger(Application.class.getName());

  public static void main(String[] args) throws Exception {
    new Application().run(args);
  }

  // enable environment variables
  @Override
  public void initialize(Bootstrap<ApplicationConfig> bootstrap) {
    bootstrap.setConfigurationSourceProvider(
        new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
            new EnvironmentVariableSubstitutor(false)));
    super.initialize(bootstrap);
  }

  @Override public void run(ApplicationConfig applicationConfig, Environment environment) {
    log.info("Configuration:\n "+ applicationConfig);
    environment.healthChecks().register("APIHealthCheck", new ApplicationHealthCheck());



    // register REST endpoints
    //environment.jersey().register(messageResources);

  }
}
