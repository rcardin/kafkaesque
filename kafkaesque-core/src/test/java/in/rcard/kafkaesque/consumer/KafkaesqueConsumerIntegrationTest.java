package in.rcard.kafkaesque.consumer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import in.rcard.kafkaesque.consumer.KafkaesqueConsumer.DelegateCreationInfo;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class KafkaesqueConsumerIntegrationTest {

  private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

  @Container
  private final KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

  private String brokerUrl;

  private KafkaProducer<String, String> producer;

  @BeforeEach
  void setUp() {
    brokerUrl = kafka.getBootstrapServers();

    setUpProducer();
  }

  private void setUpProducer() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    this.producer = new KafkaProducer<>(props);
  }

  @Test
  void pollShouldReturnTheListOfConsumerRecords() throws ExecutionException, InterruptedException {

    final String topic = "test-topic-1";
    producer.send(new ProducerRecord<>(topic, "1", "data1")).get();
    producer.send(new ProducerRecord<>(topic, "2", "data2")).get();

    KafkaesqueConsumer<String, String> consumer =
        new KafkaesqueConsumer<>(
            brokerUrl,
            5L,
            TimeUnit.SECONDS,
            5,
            50L,
            TimeUnit.MILLISECONDS,
            new DelegateCreationInfo<>(
                topic, STRING_DESERIALIZER, STRING_DESERIALIZER, new Properties()));

    final AssertionsOnConsumedDelegate<String, String> consumed = consumer.poll();
    consumed.havingRecordsSize(2);

    consumer.close();
  }

  @Test
  void pollShouldThrowAKafkaesqueConsumerPollExceptionIfSomethingWentWrongDuringThePolling()
      throws ExecutionException, InterruptedException {

    final String topic = "test-topic-2";
    producer.send(new ProducerRecord<>(topic, "3", "data3")).get();

    KafkaesqueConsumer<Integer, String> consumer =
        new KafkaesqueConsumer<>(
            brokerUrl,
            1000L,
            TimeUnit.MILLISECONDS,
            5,
            50L,
            TimeUnit.MILLISECONDS,
            new DelegateCreationInfo<>(
                topic, new IntegerDeserializer(), new StringDeserializer(), new Properties()));

    assertThatThrownBy(consumer::poll)
        .isInstanceOf(KafkaesqueConsumerPollException.class)
        .hasMessage("Error during the poll operation")
        .hasCauseInstanceOf(RuntimeException.class)
        .getCause()
        .hasMessage(
            "Error deserializing key/value for partition test-topic-2-0 at offset 0. If needed, please seek past the record to continue consumption.");

    consumer.close();
  }

  @Test
  void pollShouldThrowAnAssertionErrorIfTheConditionsOnTheEmptyPollAreNotMetAndNoMessagesWasRead() {
    KafkaesqueConsumer<String, String> consumer =
        new KafkaesqueConsumer<>(
            brokerUrl,
            1000L,
            TimeUnit.MILLISECONDS,
            20,
            50L,
            TimeUnit.MILLISECONDS,
            new DelegateCreationInfo<>(
                "test-topic-3",
                new StringDeserializer(),
                new StringDeserializer(),
                new Properties()));

    assertThatThrownBy(consumer::poll)
        .isInstanceOf(AssertionError.class)
        .hasMessage(
            "The consumer cannot find any message during the given time interval: 1000 MILLISECONDS");

    consumer.close();
  }

  @Test
  void pollShouldThrowAnAssertionErrorIfTheConditionsOnTheEmptyPollAreNotMetAndSomeMessagesWasRead()
      throws ExecutionException, InterruptedException {

    final String topic = "test-topic-4";
    producer.send(new ProducerRecord<>(topic, "4", "data4")).get();
    producer.send(new ProducerRecord<>(topic, "5", "data5")).get();

    KafkaesqueConsumer<String, String> consumer =
        new KafkaesqueConsumer<>(
            brokerUrl,
            1000L,
            TimeUnit.MILLISECONDS,
            20,
            50L,
            TimeUnit.MILLISECONDS,
            new DelegateCreationInfo<>(
                topic, new StringDeserializer(), new StringDeserializer(), new Properties()));

    assertThatThrownBy(consumer::poll)
        .isInstanceOf(AssertionError.class)
        .hasMessage(
            "The consumer reads new messages until the end of the given time interval: 1000 MILLISECONDS");

    consumer.close();
  }
}
