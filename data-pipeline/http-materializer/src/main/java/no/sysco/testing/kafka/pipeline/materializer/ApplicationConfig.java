package no.sysco.testing.kafka.pipeline.materializer;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class ApplicationConfig extends Configuration {

  @Valid @JsonProperty("name") private String name;
  @Valid @JsonProperty("kafka") private KafkaClientFactory kafkaClientFactory;

  public ApplicationConfig(String name, KafkaClientFactory kafkaClientFactory) {
    this.name = name;
    this.kafkaClientFactory = kafkaClientFactory;
  }
  public ApplicationConfig() { }

  public String getName() { return name; }
  public KafkaClientFactory getKafkaClientFactory() { return kafkaClientFactory; }
  @Override public String toString() {
    return "ApplicationConfig{" +
        "name='" + name + '\'' +
        ", kafkaClientFactory=" + kafkaClientFactory +
        '}';
  }

  public static class KafkaClientFactory<K, V> {

    @Valid @NotNull private String bootstrapServers;
    @Valid @NotNull private String schemaRegistryUrl;
    @Valid @NotNull private String sourceTopic;

    public KafkaClientFactory() {}

    public KafkaClientFactory(String bootstrapServers, String schemaRegistryUrl,
        String sourceTopic) {
      this.bootstrapServers = bootstrapServers;
      this.schemaRegistryUrl = schemaRegistryUrl;
      this.sourceTopic = sourceTopic;
    }

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
    public String getSourceTopic() { return sourceTopic; }
    public void setSourceTopic(String sourceTopic) { this.sourceTopic = sourceTopic; }
    public String getSchemaRegistryUrl() { return schemaRegistryUrl; }
    public void setSchemaRegistryUrl(String schemaRegistryUrl) { this.schemaRegistryUrl = schemaRegistryUrl; }

    @Override public String toString() {
      return "KafkaClientFactory{" +
          "bootstrapServers='" + bootstrapServers + '\'' +
          ", schemaRegistryUrl='" + schemaRegistryUrl + '\'' +
          ", sourceTopic='" + sourceTopic + '\'' +
          '}';
    }
  }
}
