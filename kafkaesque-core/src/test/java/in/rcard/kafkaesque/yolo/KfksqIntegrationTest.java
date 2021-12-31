package in.rcard.kafkaesque.yolo;

import static org.assertj.core.api.Assertions.assertThat;

import in.rcard.kafkaesque.producer.KafkaesqueProducer;
import in.rcard.kafkaesque.yolo.OutputTopic.Message;
import java.time.Duration;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
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
            .createInputTopic(PRODUCER_TEST_TOPIC_2, new IntegerSerializer(), new StringSerializer());
    inputTopic.pipeRecordsList(
        List.of(
            KafkaesqueProducer.Record.of(100, "One hundred"),
            KafkaesqueProducer.Record.of(200, "Two hundred")
        )
    );
    final ConsumerRecords<Integer, String> polled =
        consumer.poll(Duration.ofMinutes(1L));
    assertThat(polled).hasSize(2);
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
    producer.send(new ProducerRecord<>(CONSUMER_TEST_TOPIC_1, 300, "Three hundred"));
    producer.send(new ProducerRecord<>(CONSUMER_TEST_TOPIC_1, 400, "Four hundred"));
    final OutputTopic<Integer, String> outputTopic =
        Kfksq.at(brokerUrl)
            .createOutputTopic(
                CONSUMER_TEST_TOPIC_1, new IntegerDeserializer(), new StringDeserializer());
    final List<Message<Integer, String>> messages = outputTopic.readRecordsToList();
    assertThat(messages)
        .extracting("value")
        .containsExactlyInAnyOrder("Three hundred", "Four hundred");
  }
}
