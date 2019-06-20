# Description
Module contains examples of testing Kafka Producer/Consumer APIs with Kafka cluster in memory.
Testing kafka-client application(s) in isolation.

Usually developers write tests for business logic of application where producer or consumer involved.
There are no needs to write specifically unit test for producer or consumer. 
Current examples shows embedded-kafka-cluster at test.  

`NB!`: To enable some of client properties, cluster should be setup with minimum 3 brokers. 
Such as processing guarantee [exactly_once](https://kafka.apache.org/22/documentation/streams/developer-guide/config-streams.html#processing-guarantee).