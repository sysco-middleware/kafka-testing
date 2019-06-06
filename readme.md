[![Build Status](https://www.travis-ci.org/sysco-middleware/kafka-testing.svg?branch=master)](https://www.travis-ci.org/sysco-middleware/kafka-testing)
# Kafka-clients: writing automated tests
Run all tests:
```
./mvnw clean install -DskipIntegrationTests=false
```

## Modules and approaches
1. [streams-client module](./streams-client) contains examples of unit-tests for kafka-streams topologies with [kafka-streams-test-utils](https://kafka.apache.org/21/documentation/streams/developer-guide/testing.html). 
Approach covers testing topologies (stateful & stateless processors) with different `serdes` including [avro](https://avro.apache.org/docs/1.8.2/spec.html) and [confluent schema registry](https://docs.confluent.io/current/schema-registry/index.html).
2. [embedded-kafka-cluster module](./embedded-cluster) is example of kafka-embedded cluster in memory (1 Zookeeper, 1 Kafka broker, 1 Confluent schema registry). Embedded kafka cluster is used for integration test of kafka-client application. 
3. [consumer-producer-clients module](./consumer-producer-clients) contains examples of integration tests with [embedded kafka cluster](./embedded-cluster) and kafka based applications with [Producer/Consumer API](https://kafka.apache.org/documentation)  
4. [data-pipeline module](./data-pipeline) contains examples of integration tests with [embedded kafka cluster](./embedded-cluster), [wire-mock](http://wiremock.org) and kafka based applications with [Streams API](https://kafka.apache.org/documentation/streams/)
5. [e2e module](./e2e) contains `IT` test for data pipeline, using [testcontainers](https://www.testcontainers.org)


### TODO:
- [ ] Makefile
- [ ] update vers java  `8 -> 12`  `!NB`: Reflection use -> only java8 currently 
- [ ] update vers junit `4 -> 5` 

### Important notes
 - [Confluent Platform and Apache Kafka Compatibility](https://docs.confluent.io/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility)

### References
- [Apache Kafka. Developer guide. Testing](https://kafka.apache.org/20/documentation/streams/developer-guide/testing.html)
- [Getting Your Feet Wet with Stream Processing – Part 2: Testing Your Streaming Application](https://www.confluent.io/blog/stream-processing-part-2-testing-your-streaming-application)