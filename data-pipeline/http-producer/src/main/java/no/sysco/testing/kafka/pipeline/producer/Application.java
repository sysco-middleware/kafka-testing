package no.sysco.testing.kafka.pipeline.producer;

import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.logging.Logger;
import no.sysco.testing.kafka.pipeline.producer.domain.MessageRepresentationTransformer;
import no.sysco.testing.kafka.pipeline.producer.infrastructure.kafka.KafkaMessageProducer;
import no.sysco.testing.kafka.pipeline.producer.interfaces.rest.MessageResources;

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

  @Override public void run(ApplicationConfig applicationConfig, Environment environment)
      throws Exception {
    log.info("Configuration:\n "+ applicationConfig);

    KafkaMessageProducer messageProducer = new KafkaMessageProducer(applicationConfig);
    MessageRepresentationTransformer transformer = new MessageRepresentationTransformer();
    MessageResources messageResources = new MessageResources(messageProducer, transformer);

    // register REST endpoints
    environment.jersey().register(messageResources);

  }
}
