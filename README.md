![](https://github.com/rcardin/kafkaesque/workflows/Kafkaesque/badge.svg)
[![GitHub Package Registry version](https://img.shields.io/github/v/release/rcardin/kafkaesque)]()

# Kafkaesque
_Kafkaesque_ is a test library whose aim is to make the experience in testing Kafka application less 
painful. By now, the project is in its early stage, defining the API that we will implement in the near 
future.

Every help will be very useful :)

The library allows to test the following use cases:

## Use Case 1: The Application Produces Some Messages on a Topic

The first use case tests the messages produced by an application, reading them from the topic. The
code, producing the messages, is external to _Kafkaesque_. Through _Kafkaesque_, it is possible to assert
some properties on the messages generated by the application.

```
Kafkaesque
  .usingBroker(embeddedBroker)
  .<Key, Value>consume() // <-- create the builder KafkaesqueConsumerBuilder
  .fromTopic("topic-name")
  .withDeserializers(keyDeserializer, valueDeserializer)
  .waitingAtMost(10, SECONDS)
  .waitingEmptyPolls(2, 50L, MILLISECONDS)
  .expecting() // <-- build method that effectively consumes new KafkaesqueConsumer().poll()
  .havingRecordsSize(3) // <-- from here we use a ConsumedResult
  .havingHeaders(headers -> {
    // Assertions on headers
  })
  .havingKeys(keys -> {
    // Assertions on keys
  })
  .havingPayloads(payloads -> {
    // Asserions on payloads
  })
  .havingConsumerRecords(records -> {
    // Assertions on the full list of ConsumerRecord<Key, Value>
  })
  .assertingThatPayloads(contains("42")) // Uses Hamcrest.Matchers on collections :)
  .andCloseConsumer();
```

## Use Case 2: The Application Consumes Some Messages from a Topic

The second use case tests an application that reads messages from a topic. _Kafkaesque_ is 
responsible to produce such messages to trigger the execution of the application. It is also 
possible to assert conditions on the system state after the consumption of the messages.

```
Kafkaesque
  .usingBroker(embeddedBroker)
  .<Key, Value>produce() // <-- create the builder KafkaesqueProducerBuilder
  .toTopic("topic-name")
  .withDeserializers(keyDeserializer, valueDeserializer)
  .messages( /* Some list of messages */)
  .waitingAtMostForEachAck(100, MILLISECONDS) // Waiting time for each ack from the broker
  .waitingForTheConsumerAtMost(10, SECONDS) // Waiting time for the consumer to read one / all the messages
  .expecting() // build method that effectively create a producer
  .assertingAfterEach(message -> { // <- This method produce a message and asserts imme
    // Assertions on the consumer process after the sending of each message
  })
  .assertingAfterAll(messages -> {
    // Assertions on the consumer process after the sending of all the messages
  });
```

## Use Case 3: Synchronize on Produced or Consumed Messages and Test Them Outside Kafkaesque

The [`kafka-streams-test-utils`](https://kafka.apache.org/documentation/streams/developer-guide/testing.html) testing library offers to developers some useful and powerful 
abstractions. Indeed, the `TestInputTopic` and the `TestOutputTopic` let developers to manage 
asynchronous communication with a broker as it is fully synchronous. In this case, the library does
not start any broker, not even embedded.

Kafkaesque **will** offer to developers the same abstractions, trying to achieve the same 
synchronous behavior. 

```
var kafkaesque = Kafkaesque.usingBroker(embeddedBroker);
var inputTopic = kafkaesque.createInputTopic("inputTopic", keySerializer, valueSerializer);
inputTopic.pipeInput("key", "value");

var outputTopic = kafkaesque.createOutputTopic("outputTopic", keyDeserializer, valueDeserializer);
var records = outputTopic.readRecordsToList();
```

The feature is currently under development.

## Modules

### Core module

The _Kafkaesque_ library contains many submodules. The `core` module contains the interfaces and 
agnostic concrete classes offering the above fluid API.

In detail, the `core` module uses the [Awaitility](http://www.awaitility.org/) Java library to deal
with the asynchronicity nature of each of the above use cases.

### Kafkaesque for Spring Kafka

The only concrete instantiation of the library is the `kafkaesque-spring-kafka` module, which adds 
features to the `spring-kafka-test` library made by Spring.

Add the following dependency to your `pom.xml` file to use the `kafkaesque-spring-kafka`.

```xml
<dependency>
	<groupId>in.rcard</groupId>
	<artifactId>kafkaesque-spring-kafka</artifactId>
	<version>0.2.0-SNAPSHOT</version>
	<scope>test</scope>
</dependency>
```

Since the library is published in the GitHub Packages repository, you need to add also the following
definition to your `pom.xml`:

```xml
<repository>
	<id>github</id>
	<name>GitHub rcardin Apache Maven Packages</name>
	<url>https://maven.pkg.github.com/rcardin/kafkaesque</url>
</repository>
``` 

#### JUnit 4

Create a test for JUnit 4 is easy, just create an instance of the `SpringKafkaesqueRule` class:

```java
@SpringBootTest
public class SpringKafkaesqueRuleTest {
  @ClassRule
  public static final SpringKafkaesqueRule kafkaesqueRule =
      new SpringKafkaesqueRule(1, false, "test");

  @Test
  public void aTest() {
    // Code producing messages
    // ...
    kafkaesqueRule.getKafkaesque()
        .<Integer, String>consume()
        .fromTopic("test")
        .waitingAtMost(1L, TimeUnit.SECONDS)
        .waitingEmptyPolls(5, 100L, TimeUnit.MILLISECONDS)
        .withDeserializers(new IntegerDeserializer(), new StringDeserializer())
        .expecting()
        .havingRecordsSize(2)
        .assertingThatPayloads(Matchers.containsInAnyOrder("data1", "data2"))
        .andCloseConsumer();
  }
}
```

#### JUnit 5

Instead, to create a test for the JUnit 5 test framework we need first to create the instance of the
`EmbeddedKafkaBroker`.

```java
@SpringBootTest(classes = {TestConfiguration.class})
@EmbeddedKafka(
    topics = {
        SpringKafkaesqueTest.CONSUMER_TEST_TOPIC, 
        SpringKafkaesqueTest.PRODUCER_TEST_TOPIC
    }
)
class SpringKafkaesqueTest {

  static final String CONSUMER_TEST_TOPIC = "test";
  static final String PRODUCER_TEST_TOPIC = "test1";

  @Autowired 
  private EmbeddedKafkaBroker broker;

  @Test
  void consumeShouldConsumeMessagesProducesFromOutsideProducer() {
    // Code producing messages
    // ...
    Kafkaesque
        .usingBroker(broker)
        .<Integer, String>consume()
        .fromTopic(CONSUMER_TEST_TOPIC)
        .waitingAtMost(1L, TimeUnit.SECONDS)
        .waitingEmptyPolls(5, 100L, TimeUnit.MILLISECONDS)
        .withDeserializers(new IntegerDeserializer(), new StringDeserializer())
        .expecting()
        .havingRecordsSize(2)
        .assertingThatPayloads(Matchers.containsInAnyOrder("data1", "data2"))
        .andCloseConsumer();
  }
}
```


