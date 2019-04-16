package no.sysco.testing.kafka.pipeline.producer;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class ProducerRestConfig extends Configuration {

  @Valid
  @JsonProperty("name")
  private String name;

  @Valid
  @JsonProperty("kafka")
  private KafkaClientFactory kafkaClientFactory;

  public ProducerRestConfig(String name, KafkaClientFactory kafkaClientFactory) {
    this.name = name;
    this.kafkaClientFactory = kafkaClientFactory;
  }

  public ProducerRestConfig() {}

  public String getName() {
    return name;
  }

  public KafkaClientFactory getKafkaClientFactory() {
    return kafkaClientFactory;
  }

  @Override
  public String toString() {
    return "ProducerRestConfig{"
        + "name='"
        + name
        + '\''
        + ", kafkaClientFactory="
        + kafkaClientFactory
        + '}';
  }

  public static class KafkaClientFactory<K, V> {

    @Valid @NotNull private String bootstrapServers;
    @Valid @NotNull private String schemaRegistryUrl;
    @Valid @NotNull private String sinkTopic;

    public KafkaClientFactory() {}

    public KafkaClientFactory(String bootstrapServers, String schemaRegistryUrl, String sinkTopic) {
      this.bootstrapServers = bootstrapServers;
      this.schemaRegistryUrl = schemaRegistryUrl;
      this.sinkTopic = sinkTopic;
    }

    public String getBootstrapServers() {
      return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
    }

    public String getSinkTopic() {
      return sinkTopic;
    }

    public void setSinkTopic(String sinkTopic) {
      this.sinkTopic = sinkTopic;
    }

    public String getSchemaRegistryUrl() {
      return schemaRegistryUrl;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
      this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public String toString() {
      return "KafkaClientFactory{"
          + "bootstrapServers='"
          + bootstrapServers
          + '\''
          + ", schemaRegistryUrl='"
          + schemaRegistryUrl
          + '\''
          + ", sinkTopic='"
          + sinkTopic
          + '\''
          + '}';
    }
  }
}
