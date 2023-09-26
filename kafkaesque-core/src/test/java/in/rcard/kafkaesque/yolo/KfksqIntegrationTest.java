package in.rcard.kafkaesque.yolo;

import static in.rcard.kafkaesque.common.Header.header;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import in.rcard.kafkaesque.common.Header;
import in.rcard.kafkaesque.producer.KafkaesqueProducer;
import in.rcard.kafkaesque.yolo.OutputTopic.Message;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
class KfksqIntegrationTest {

  static final String CONSUMER_TEST_TOPIC_1 = "test1";
  static final String PRODUCER_TEST_TOPIC_2 = "test2";

  @Container
  private final KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

  private String brokerUrl;

  private KafkaConsumer<Integer, String> consumer;
  private KafkaProducer<Integer, String> producer;

  @BeforeEach
  void setUp() {
    brokerUrl = kafka.getBootstrapServers();
    setUpConsumer();
    setUpProducer();
  }

  private void setUpConsumer() {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    this.consumer = new KafkaConsumer<>(props);
  }

  private void setUpProducer() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    this.producer = new KafkaProducer<>(props);
  }

  @AfterEach
  void tearDown() {
    consumer.close();
  }

  @Test
  void createInputTopicShouldCreateAStructureThatSendToTheBrokerTheRecords() {
    subscribeConsumerToTopic(PRODUCER_TEST_TOPIC_2);
    final InputTopic<Integer, String> inputTopic =
        Kfksq.at(brokerUrl)
            .createInputTopic(
                PRODUCER_TEST_TOPIC_2, new IntegerSerializer(), new StringSerializer());
    final Header header = header("hKey", "hValue");
    inputTopic.pipeRecordsList(
        List.of(
            KafkaesqueProducer.Record.of(100, "One hundred", header),
            KafkaesqueProducer.Record.of(200, "Two hundred", header)));

    final ConsumerRecords<Integer, String> polled = consumer.poll(Duration.ofMinutes(1L));
    final Iterable<ConsumerRecord<Integer, String>> actualConsumerRecords =
        polled.records(PRODUCER_TEST_TOPIC_2);

    Headers kafkaHeaders = new RecordHeaders();
    kafkaHeaders.add(new RecordHeader("hKey", "hValue".getBytes()));
    assertThat(actualConsumerRecords)
        .flatExtracting("key", "value", "headers")
        .containsExactlyInAnyOrder(
            100, "One hundred", kafkaHeaders, 200, "Two hundred", kafkaHeaders);
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

  @Test
  void createOutputTopicShouldCreateAStructureTheReadsFromTheBrokerTheMessages() {
    final Header header = header("hKey", "hValue".getBytes());
    final ProducerRecord<Integer, String> firstRecord =
        producerRecord(300, "Three hundred", header);
    final ProducerRecord<Integer, String> secondRecord =
        producerRecord(400, "Four hundred", header);
    producer.send(firstRecord);
    producer.send(secondRecord);

    final OutputTopic<Integer, String> outputTopic =
        Kfksq.at(brokerUrl)
            .createOutputTopic(
                CONSUMER_TEST_TOPIC_1, new IntegerDeserializer(), new StringDeserializer());
    final List<Message<Integer, String>> actualMessages = outputTopic.readRecordsToList();

    assertThat(actualMessages)
        .extracting("value", "headers")
        .containsExactlyInAnyOrder(
            tuple("Three hundred", List.of(header)), tuple("Four hundred", List.of(header)));
  }

  private ProducerRecord<Integer, String> producerRecord(
      int x, String Three_hundred, Header... headers) {
    final ProducerRecord<Integer, String> producerRecord =
        new ProducerRecord<>(CONSUMER_TEST_TOPIC_1, x, Three_hundred);
    Arrays.stream(headers).forEach(header -> producerRecord.headers().add(header.toKafkaHeader()));
    return producerRecord;
  }
}
