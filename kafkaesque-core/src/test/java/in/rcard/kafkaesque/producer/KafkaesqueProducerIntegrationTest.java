package in.rcard.kafkaesque.producer;

import static in.rcard.kafkaesque.common.Header.*;
import static org.assertj.core.api.Assertions.assertThat;

import in.rcard.kafkaesque.producer.KafkaesqueProducer.DelegateCreationInfo;
import in.rcard.kafkaesque.producer.KafkaesqueProducer.Record;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class KafkaesqueProducerIntegrationTest {

  private static final String TEST_TOPIC = "test-topic";
  private static final String TEST_TOPIC_1 = "test-topic-1";
  private static final StringSerializer STRING_SERIALIZER = new StringSerializer();

  @Container
  private final KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

  private String brokerUrl;

  private KafkaConsumer<String, String> consumer;

  private KafkaesqueProducer<String, String> producer;

  @BeforeEach
  void setUp() {
    brokerUrl = kafka.getBootstrapServers();

    setUpConsumer();
  }

  private void setUpConsumer() {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    this.consumer = new KafkaConsumer<>(props);
  }

  @AfterEach
  void tearDown() {
    consumer.close();
  }

  @Test
  void sendRecordShouldSendASingleRecordToKafkaTopic() {
    subscribeConsumerToTopic(TEST_TOPIC);
    setUpKafkaesqueProducer(TEST_TOPIC);

    producer.sendRecord(Record.of("key1", "value1"));

    final ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMinutes(1L));
    assertThat(polled)
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue("key", "key1")
        .hasFieldOrPropertyWithValue("value", "value1");
  }

  @Test
  void sendRecordsShouldSendAListOfRecordsToKafkaTopic() {
    subscribeConsumerToTopic(TEST_TOPIC_1);
    setUpKafkaesqueProducer(TEST_TOPIC_1);

    producer.sendRecords(List.of(Record.of("key1", "value1"), Record.of("key2", "value2")));

    final ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMinutes(1L));
    assertThat(polled).hasSize(2);
  }

  @Test
  void sendRecordsWithHeadersShouldSendARecordToKafkaTopic() {
    subscribeConsumerToTopic(TEST_TOPIC);
    setUpKafkaesqueProducer(TEST_TOPIC);

    producer.sendRecord(Record.of("key1", "value1", header("hKey", "hValue")));

    final ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMinutes(1L));
    assertThat(polled)
            .hasSize(1)
            .first()
            .hasFieldOrPropertyWithValue("key", "key1")
            .hasFieldOrPropertyWithValue("value", "value1")
            .extracting(cr -> cr.headers().lastHeader("hKey"))
            .isEqualTo(new RecordHeader("hKey", "hValue".getBytes()));
  }

  private void subscribeConsumerToTopic(String topic) {
    CountDownLatch latch = new CountDownLatch(1);
    consumer.subscribe(
        List.of(topic),
        new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            latch.countDown();
          }
        });

    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .until(
            () -> {
              consumer.poll(Duration.ofMillis(100));
              return latch.getCount() == 0;
            });
  }

  private void setUpKafkaesqueProducer(String topic) {
    producer =
        new KafkaesqueProducer<>(
            brokerUrl,
            Duration.of(200L, ChronoUnit.MILLIS),
            new DelegateCreationInfo<>(
                topic, STRING_SERIALIZER, STRING_SERIALIZER, new Properties()));
  }
}
