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

  private final DatabaseWebService dbWebService;
  private final MessageRepresentationTransformer transformer;
  private final KafkaMessageMaterializer kafkaMessageMaterializer;

  public MaterializerApplication(MaterializerConfig materializerConfig, boolean inMemory) {
    this.dbWebService =
        inMemory
            ? new DatabaseWebServiceInMemmory()
            : new DatabaseWebServiceRest(materializerConfig);
    this.transformer = new MessageRepresentationTransformer();
    this.kafkaMessageMaterializer =
        new KafkaMessageMaterializer(materializerConfig, dbWebService, transformer);
  }

  public static void main(String[] args) {
    // typesafe conf load
    final Config config = ConfigFactory.load();
    final MaterializerConfig materializerConfig = MaterializerConfig.loadConfig(config);

    final MaterializerApplication materializerApplication =
        new MaterializerApplication(materializerConfig, false);

    materializerApplication.start();
  }

  public void stop() {
    kafkaMessageMaterializer.stop();
  }

  public void start() {
    kafkaMessageMaterializer.run();
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaMessageMaterializer::stop));
  }

  // for test
  DatabaseWebService getDbWebService() {
    return dbWebService;
  }

  KafkaMessageMaterializer getKafkaMessageMaterializer() {
    return kafkaMessageMaterializer;
  }
}
