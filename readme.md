# Kafka-clients: writing automated tests

## Clients
- [X] Streams API (Stateless, Stateful)
- [ ] Consumer/Producer API
- [ ] Data pipelines

### Streams API
- [X] Topology using dsl + local storage + avro schemas
- [ ] Topology using processor api + local storage + avro schemas

### Consumer/Producer API
- [ ] Test with embedded kafka

### Data pipelines
- [ ] Testcontainers
- [ ] Embedded kafka

### TODO:
- update vers java  `8 -> 12` 
- update vers junit `4 -> 5` 

`!NB`: Reflection use -> only java8 currently  

### Important notes
 - [Confluent Platform and Apache Kafka Compatibility](https://docs.confluent.io/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility)

### References
- [Apache Kafka. Developer guide. Testing](https://kafka.apache.org/20/documentation/streams/developer-guide/testing.html)