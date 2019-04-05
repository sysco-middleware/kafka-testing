package no.sysco.testing.kafka.pipeline.materializer;

import com.codahale.metrics.health.HealthCheck;

public class ApplicationHealthCheck extends HealthCheck {

  @Override protected Result check() {
    return Result.healthy();
  }
}
