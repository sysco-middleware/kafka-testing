package no.sysco.testing.kafka.pipeline.materializer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.logging.Logger;
import no.sysco.testing.kafka.pipeline.materializer.domain.MessageRepresentationTransformer;
import no.sysco.testing.kafka.pipeline.materializer.infrastructure.kafka.KafkaMessageMaterializer;
import no.sysco.testing.kafka.pipeline.materializer.infrastructure.service.DatabaseWebService;
import no.sysco.testing.kafka.pipeline.materializer.infrastructure.service.DatabaseWebServiceInMemmory;
import no.sysco.testing.kafka.pipeline.materializer.infrastructure.service.DatabaseWebServiceRest;

public class MaterializerApplication {
  private static Logger log = Logger.getLogger(MaterializerApplication.class.getName());

  public static void main(String[] args) {
    // typesafe conf load
    final Config config = ConfigFactory.load();
    final MaterializerConfig materializerConfig = MaterializerConfig.loadConfig(config);
    run(materializerConfig, false);
  }

  public static void run(MaterializerConfig materializerConfig, boolean inMemory) {
    final DatabaseWebService dbWebService = inMemory
        ? new DatabaseWebServiceInMemmory()
        : new DatabaseWebServiceRest(materializerConfig);

    final MessageRepresentationTransformer transformer = new MessageRepresentationTransformer();
    final KafkaMessageMaterializer kafkaMessageMaterializer = new KafkaMessageMaterializer(
        materializerConfig,
        dbWebService,
        transformer
    );

    kafkaMessageMaterializer.run();
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaMessageMaterializer::stop));
  }
}
