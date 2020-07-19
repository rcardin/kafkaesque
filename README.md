# kafkaesque
A test library which aim is to make the experience in testing Kafka application less painful

The library allows to test the following use cases:

## Use Case 1: The application produces some messages on a topic
```java
kafkaEmbedded
	.withConsumer(topic, deserializer)
	.consume()
	.waitingAtMost(10, SECONDS)
	.recordsSize(3)
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

TODO
