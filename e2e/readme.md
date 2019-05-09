# End to end tests (e2e)
Module contains IT test for whole [data pipeline](../data-pipeline) using [testcontainers-java]()

## Approach 1: Setup each container for data-pipeline
### Topic creation, before tests
* [testcontainers - kafka container](https://www.testcontainers.org/modules/kafka/)  
* [testcontainers - execInContainer](https://www.testcontainers.org/features/commands/)
```java
private static void createTopic(String topicName) {
  // kafka container uses with embedded zookeeper
  // confluent platform and Kafka compatibility 5.1.x <-> kafka 2.1.x
  // kafka 2.1.x require option --zookeeper, later versions use --bootstrap-servers instead
  String createTopic =
      String.format(
          "/usr/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic %s",
          topicName);
  try {
    final Container.ExecResult execResult = kafka.execInContainer("/bin/sh", "-c", createTopic);
    if (execResult.getExitCode() != 0) fail();
  } catch (Exception e) {
    e.printStackTrace();
    fail();
  }
}
```

## Approach 2: Use docker-compose 
### Topic creation, before tests  
* [Confluent Platform Utility Belt (cub)](https://docs.confluent.io/current/installation/docker/development.html#confluent-platform-utility-belt-cub) 

To create topics, I run additional container (used NOT as kafka broker).
 
```yaml
# This "container" is a workaround to pre-create topics
# https://github.com/confluentinc/examples/blob/f854ac008952346ceaba0d1f1a352788f0572c74/microservices-orders/docker-compose.yml#L182-L215
kafka-setup:
  image: confluentinc/cp-kafka:5.1.2
  hostname: kafka-setup
  container_name: kafka-setup
  depends_on:
    - kafka
    - zookeeper
  command: "bash -c 'echo Waiting for Kafka to be ready... && \
               cub kafka-ready -b kafka:9092 1 30 && \
               kafka-topics --create --if-not-exists --zookeeper zookeeper:2181 --partitions 1 --replication-factor 1 --topic events-message-v1 && \
               sleep 60'"
  environment:
    # The following settings are listed here only to satisfy the image's requirements.
    # We override the image's `command` anyways, hence this container will not start a broker.
    KAFKA_BROKER_ID: ignored
    KAFKA_ZOOKEEPER_CONNECT: ignored
``` 