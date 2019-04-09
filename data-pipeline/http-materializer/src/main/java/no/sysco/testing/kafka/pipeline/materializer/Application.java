package no.sysco.testing.kafka.pipeline.materializer;

import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;
import no.sysco.testing.kafka.pipeline.materializer.domain.MessageRepresentationTransformer;
import no.sysco.testing.kafka.pipeline.materializer.infrastructure.kafka.KafkaMessageMaterializer;
import no.sysco.testing.kafka.pipeline.materializer.infrastructure.service.DatabaseWebService;
import no.sysco.testing.kafka.pipeline.materializer.infrastructure.service.DatabaseWebServiceInMemmory;
import no.sysco.testing.kafka.pipeline.materializer.infrastructure.service.DatabaseWebServiceRest;

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
    environment.healthChecks().register(applicationConfig.getName()+"HealthCheck", new ApplicationHealthCheck());

    //final DatabaseWebService dbWebService = new DatabaseWebServiceInMemmory();
    final DatabaseWebService dbWebService = new DatabaseWebServiceRest(applicationConfig);
    final MessageRepresentationTransformer transformer = new MessageRepresentationTransformer();
    final KafkaMessageMaterializer kafkaMessageMaterializer = new KafkaMessageMaterializer(
        applicationConfig,
        dbWebService,
        transformer
    );

    final ExecutorService executorService = environment.lifecycle()
        .executorService("kafka-message-materializer")
        // horizontal scaling on app level, up to 10 threads, depends on source-topic partitions amount
        .maxThreads(10)
        .build();
    executorService.submit(kafkaMessageMaterializer);
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaMessageMaterializer::stop));
  }
}
