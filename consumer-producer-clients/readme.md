# Description
Module contains examples of testing Kafka Producer/Consumer APIs with Kafka cluster in memory.
Testing kafka-client application(s) in isolation.

`NB!`: Some of client properties have specific requirements about how cluster should be setup. 
Example: [processing.guarantee:exactly_once](https://kafka.apache.org/22/documentation/streams/developer-guide/config-streams.html#processing-guarantee)

## TODO
Module contains examples of testing Kafka Consumer/Producer APIs with: 
- [X] embedded kafka + producer/consumer
- [X] embedded kafka + producer/consumer + avro
- [X] cleanup test

## Related Issues
* [issue-#26](https://github.com/confluentinc/kafka-streams-examples/issues/26)
 
## Additional Info
* [scalatest-embedded-kafka](https://github.com/manub/scalatest-embedded-kafka) 
* [kafka-it-embedded](https://github.com/asmaier/mini-kafka/blob/master/src/test/java/de/am/KafkaProducerIT.java) 
* [embedded-single-node-kafka-cluster](https://www.confluent.io/blog/stream-processing-part-2-testing-your-streaming-application)
 