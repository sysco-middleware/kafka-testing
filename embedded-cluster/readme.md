# Description
Module contains copy of code from [kafka-streams-example](https://github.com/confluentinc/kafka-streams-examples/blob/5.2.0-post/src/test/java/io/confluent/examples/streams/kafka/EmbeddedSingleNodeKafkaCluster.java)
There is open issue: extract embedded kafka for testing purpose.  

`EmbeddedSingleNodeKafkaCluster` contains: Zookeeper, KafkaBroker and SchemaRegistry.

## Important notes
* Schema registry at start call some deprecated methods, check logs
* Current support java8 only, because there are reflections usage under the hood

## Related Issues
* [issue-#26: Encapsulate EmbeddedSingleNodeKafkaCluster in a seperately-available maven/gradle/sbt dep](https://github.com/confluentinc/kafka-streams-examples/issues/26)
* [KIP-139: Kafka TestKit library](https://cwiki.apache.org/confluence/display/KAFKA/KIP-139%3A+Kafka+TestKit+library)