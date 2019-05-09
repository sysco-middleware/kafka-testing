[![Build Status](https://www.travis-ci.org/sysco-middleware/kafka-testing.svg?branch=master)](https://www.travis-ci.org/sysco-middleware/kafka-testing)
# Kafka-clients: writing automated tests [WIP]
Run all tests:
```
./mvnw clean install -DskipIntegrationTests=false
```
## Test approaches
1. [kafka-streams-test-utils](https://kafka.apache.org/21/documentation/streams/developer-guide/testing.html) 
for `unit` testing Topologies in [streams-client module](./streams-client). 
Approach covers testing topologies (stateful & stateless processors) with different `serdes` including [avro](https://avro.apache.org/docs/1.8.2/spec.html) use-cases with [confluent schema registry](https://docs.confluent.io/current/schema-registry/index.html).
2. [embedded-kafka-cluster](./consumer-producer-clients) for `unit testing` and `IT` kafka-client application in isolation. 
3. [test-containers](https://github.com/testcontainers/testcontainers-java) for `IT` and `end-to-end` tests.
4. [data-pipeline](./data-pipeline) different approaches how to make `IT` tests.
5. [e2e](./e2e) end-to-end test for data pipeline.

### TODO:
- [ ] Makefile
- [ ] update vers java  `8 -> 12`  `!NB`: Reflection use -> only java8 currently 
- [ ] update vers junit `4 -> 5` 

### Important notes
 - [Confluent Platform and Apache Kafka Compatibility](https://docs.confluent.io/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility)

### References
- [Apache Kafka. Developer guide. Testing](https://kafka.apache.org/20/documentation/streams/developer-guide/testing.html)
- [Getting Your Feet Wet with Stream Processing – Part 2: Testing Your Streaming Application](https://www.confluent.io/blog/stream-processing-part-2-testing-your-streaming-application)