package no.sysco.testing.kafka.pipeline.producer;

import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import no.sysco.testing.kafka.pipeline.producer.domain.MessageRepresentationTransformer;
import no.sysco.testing.kafka.pipeline.producer.infrastructure.kafka.KafkaMessageProducer;
import no.sysco.testing.kafka.pipeline.producer.interfaces.rest.MessageResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerRestApplication extends io.dropwizard.Application<ProducerRestConfig> {

  private static Logger log = LoggerFactory.getLogger(ProducerRestApplication.class);

  public static void main(String[] args) throws Exception {
    new ProducerRestApplication().run(args);
  }

  // enable environment variables
  @Override
  public void initialize(Bootstrap<ProducerRestConfig> bootstrap) {
    bootstrap.setConfigurationSourceProvider(
        new SubstitutingSourceProvider(
            bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor(false)));
    super.initialize(bootstrap);
  }

  @Override
  public void run(ProducerRestConfig producerRestConfig, Environment environment) {
    log.info("Configuration: {}\n", producerRestConfig);
    environment
        .healthChecks()
        .register(producerRestConfig.getName() + "HealthCheck", new ApplicationHealthCheck());

    KafkaMessageProducer messageProducer = new KafkaMessageProducer(producerRestConfig);
    MessageRepresentationTransformer transformer = new MessageRepresentationTransformer();
    MessageResources messageResources = new MessageResources(messageProducer, transformer);

    // register REST endpoints
    environment.jersey().register(messageResources);
  }
}
