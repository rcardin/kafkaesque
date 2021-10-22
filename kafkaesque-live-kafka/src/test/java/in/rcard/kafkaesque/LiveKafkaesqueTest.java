package in.rcard.kafkaesque;

import in.rcard.kafkaesque.KafkaesqueOutputTopic.Message;
import in.rcard.kafkaesque.KafkaesqueProducer.Record;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class LiveKafkaesqueTest {
  
  static final String CONSUMER_TEST_TOPIC = "test";
  static final String PRODUCER_TEST_TOPIC = "test1";
  static final String CONSUMER_TEST_TOPIC_1 = "test2";
  static final String PRODUCER_TEST_TOPIC_1 = "test3";
  
  @Container
  private KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
  
  private String brokerUrl;
  
  private KafkaProducer<Integer, String> producer;
  private KafkaConsumer<Integer, String> consumer;
  
  @BeforeEach
  void setUp() {
    brokerUrl = kafka.getBootstrapServers();
    setUpProducer();
    setUpConsumer();
  }
  
  private void setUpProducer() {
  
  }
  
  private void setUpConsumer() {
  
  }
  
  @AfterEach
  void tearDown() {
    consumer.close();
  }
  
  @Test
  void consumeShouldConsumeMessagesProducesFromOutsideProducer() {
//    kafkaTemplate.send(CONSUMER_TEST_TOPIC, 1, "data1");
//    kafkaTemplate.send(CONSUMER_TEST_TOPIC, 2, "data2");
    new LiveKafkaesque(brokerUrl)
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
  
  @Test
  void produceShouldProduceMessageForOutsideConsumer() {
    new LiveKafkaesque(brokerUrl)
        .<Integer, String>produce()
        .messages(Arrays.asList(Record.of(1, "value1"), Record.of(2, "value2")))
        .toTopic(PRODUCER_TEST_TOPIC)
        .withSerializers(new IntegerSerializer(), new StringSerializer())
        .expecting()
        .assertingAfterEach(
            record -> {
//              final ConsumerRecord<Integer, String> consumerRecord =
//                  KafkaTestUtils.getSingleRecord(consumer, PRODUCER_TEST_TOPIC);
//              assertThat(consumerRecord)
//                  .hasFieldOrPropertyWithValue("key", record.key())
//                  .hasFieldOrPropertyWithValue("value", record.value());
            });
  }
  
  @Test
  void createInputTopicShouldCreateAStructureThatSendToTheBrokerTheRecords() {
    final KafkaesqueInputTopic<Integer, String> inputTopic =
        new LiveKafkaesque(brokerUrl)
            .createInputTopic(PRODUCER_TEST_TOPIC_1, new IntegerSerializer(), new StringSerializer());
    inputTopic.pipeRecordList(
        Arrays.asList(Record.of(100, "One hundred"), Record.of(200, "Two hundred")));
    new LiveKafkaesque(brokerUrl)
        .<Integer, String>consume()
        .fromTopic(PRODUCER_TEST_TOPIC_1)
        .waitingAtMost(1L, TimeUnit.SECONDS)
        .waitingEmptyPolls(5, 100L, TimeUnit.MILLISECONDS)
        .withDeserializers(new IntegerDeserializer(), new StringDeserializer())
        .expecting()
        .havingRecordsSize(2)
        .assertingThatPayloads(Matchers.containsInAnyOrder("One hundred", "Two hundred"))
        .andCloseConsumer();
  }
  
  @Test
  void createOutputTopicShouldCreateAStructureTheReadsFromTheBrokerTheMessages() {
//    kafkaTemplate.send(CONSUMER_TEST_TOPIC_1, 300, "Three hundred");
//    kafkaTemplate.send(CONSUMER_TEST_TOPIC_1, 400, "Four hundred");
    final KafkaesqueOutputTopic<Integer, String> outputTopic =
        new LiveKafkaesque(brokerUrl)
            .createOutputTopic(
                CONSUMER_TEST_TOPIC_1, new IntegerDeserializer(), new StringDeserializer());
    final List<Message<Integer, String>> messages = outputTopic.readRecordsToList();
//    assertThat(messages)
//        .extracting("value")
//        .containsExactlyInAnyOrder("Three hundred", "Four hundred");
  }
}