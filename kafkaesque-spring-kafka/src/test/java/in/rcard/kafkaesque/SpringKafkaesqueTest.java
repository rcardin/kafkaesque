package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;

import in.rcard.kafkaesque.yolo.InputTopic;
import in.rcard.kafkaesque.yolo.OutputTopic;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@SpringBootTest(classes = {SpringKafkaesque.class})
@EmbeddedKafka(
    topics = {
      SpringKafkaesqueTest.CONSUMER_TEST_TOPIC,
      SpringKafkaesqueTest.CONSUMER_TEST_TOPIC_1,
      SpringKafkaesqueTest.PRODUCER_TEST_TOPIC,
      SpringKafkaesqueTest.PRODUCER_TEST_TOPIC_1
    })
class SpringKafkaesqueTest {

  static final String CONSUMER_TEST_TOPIC = "test";
  static final String PRODUCER_TEST_TOPIC = "test1";
  static final String CONSUMER_TEST_TOPIC_1 = "test2";
  static final String PRODUCER_TEST_TOPIC_1 = "test3";
  
  @Autowired private EmbeddedKafkaBroker broker;

  private KafkaTemplate<Integer, String> kafkaTemplate;
  private Consumer<Integer, String> consumer;

  @BeforeEach
  void setUp() {
    setUpProducer();
    setUpConsumer();
  }

  private void setUpProducer() {
    final DefaultKafkaProducerFactory<Integer, String> producerFactory =
        new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker));
    kafkaTemplate = new KafkaTemplate<>(producerFactory);
  }

  private void setUpConsumer() {
    Map<String, Object> consumerProps =
        KafkaTestUtils.consumerProps(PRODUCER_TEST_TOPIC, "false", broker);
    DefaultKafkaConsumerFactory<Integer, String> cf =
        new DefaultKafkaConsumerFactory<>(consumerProps);
    consumer = cf.createConsumer();
    broker.consumeFromAllEmbeddedTopics(consumer);
  }

  @AfterEach
  void tearDown() {
    consumer.close();
  }

  @Test
  void consumeShouldConsumeMessagesProducesFromOutsideProducer() {
    kafkaTemplate.send(CONSUMER_TEST_TOPIC, 1, "data1");
    kafkaTemplate.send(CONSUMER_TEST_TOPIC, 2, "data2");
    new SpringKafkaesque(broker)
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
    new SpringKafkaesque(broker)
        .<Integer, String>produce()
        .messages(Arrays.asList(Record.of(1, "value1"), Record.of(2, "value2")))
        .toTopic(PRODUCER_TEST_TOPIC)
        .withSerializers(new IntegerSerializer(), new StringSerializer())
        .expecting()
        .assertingAfterEach(
            record -> {
              final ConsumerRecord<Integer, String> consumerRecord =
                  KafkaTestUtils.getSingleRecord(consumer, PRODUCER_TEST_TOPIC);
              assertThat(consumerRecord)
                  .hasFieldOrPropertyWithValue("key", record.key())
                  .hasFieldOrPropertyWithValue("value", record.value());
            });
  }

  @Test
  void createInputTopicShouldCreateAStructureThatSendToTheBrokerTheRecords() {
    final InputTopic<Integer, String> inputTopic =
        new SpringKafkaesque(broker)
            .createInputTopic(PRODUCER_TEST_TOPIC_1, new IntegerSerializer(), new StringSerializer());
    inputTopic.pipeRecordList(
        Arrays.asList(Record.of(100, "One hundred"), Record.of(200, "Two hundred")));
    new SpringKafkaesque(broker)
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
    kafkaTemplate.send(CONSUMER_TEST_TOPIC_1, 300, "Three hundred");
    kafkaTemplate.send(CONSUMER_TEST_TOPIC_1, 400, "Four hundred");
    final OutputTopic<Integer, String> outputTopic =
        new SpringKafkaesque(broker)
            .createOutputTopic(
                CONSUMER_TEST_TOPIC_1, new IntegerDeserializer(), new StringDeserializer());
    final List<Message<Integer, String>> messages = outputTopic.readRecordsToList();
    assertThat(messages)
        .extracting("value")
        .containsExactlyInAnyOrder("Three hundred", "Four hundred");
  }
}
