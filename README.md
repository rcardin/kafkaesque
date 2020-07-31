![](https://github.com/rcardin/kafkaesque/workflows/Kafkaesque/badge.svg)

# Kafkaesque
_Kafkaesque_ is a test library whose aim is to make the experience in testing Kafka application less 
painful. By now, the project is in its early stage, defining the API that we will implement in the near 
future.

Every help will be very useful :)

The library allows to test the following use cases:

## Use Case 1: The Application Produces Some Messages on a Topic
```java
kafkaesque
  .consume() // <-- create the builder KafkaesqueConsumerBuilder
  .fromTopic("topic-name")
  .withDeserializer(deserializer)
  .waitingAtMost(10, SECONDS)
  .expecting() // <-- build method that effectively consumes new KafkaesqueConsumer().poll()
  .recordsSize(3) // <-- from here we use a ConsumedResult
  .havingHeaders(headers -> {
    // Assertions on headers
  })
  .havingKeys(keys -> {
    // Assertions on keys
  })
  .havingPayloads§(payloads -> {
    // Asserions on payloads
  });
```

More to come soon!
