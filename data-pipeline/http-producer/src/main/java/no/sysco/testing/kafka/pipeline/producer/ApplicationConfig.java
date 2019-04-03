package no.sysco.testing.kafka.pipeline.producer;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public KafkaClientFactory() {}
    public KafkaClientFactory(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
    @Override public String toString() {
      return "KafkaClientFactory{" +
          "bootstrapServers='" + bootstrapServers + '\'' +
          '}';
    }
  }
}
