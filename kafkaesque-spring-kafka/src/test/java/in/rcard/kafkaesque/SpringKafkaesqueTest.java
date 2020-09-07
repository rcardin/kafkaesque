package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;

import in.rcard.kafkaesque.SpringKafkaesqueTest.TestConfiguration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@SpringBootTest(classes = {TestConfiguration.class})
@EmbeddedKafka(
    topics = {SpringKafkaesqueTest.CONSUMER_TEST_TOPIC, SpringKafkaesqueTest.PRODUCER_TEST_TOPIC})
class SpringKafkaesqueTest {

  static final String CONSUMER_TEST_TOPIC = "test";
  static final String PRODUCER_TEST_TOPIC = "test1";

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
    kafkaTemplate.setDefaultTopic(CONSUMER_TEST_TOPIC);
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
    kafkaTemplate.sendDefault(1, "data1");
    kafkaTemplate.sendDefault(2, "data2");
    new SpringKafkaesque(broker)
        .<Integer, String>consume()
        .fromTopic(CONSUMER_TEST_TOPIC)
        .waitingAtMost(1L, TimeUnit.SECONDS)
        .waitingEmptyPolls(5, 100L, TimeUnit.MILLISECONDS)
        .withDeserializers(new IntegerDeserializer(), new StringDeserializer())
        .expecting()
        .poll()
        .havingRecordsSize(2)
        .assertingThatPayloads(Matchers.containsInAnyOrder("data1", "data2"));
  }

  @Test
  void produceShouldProduceMessageForOutsideConsumer() {
    new SpringKafkaesque(broker)
        .<Integer, String>produce()
        .messages(
            Arrays.asList(
                new ProducerRecord<>(PRODUCER_TEST_TOPIC, 1, "value1"),
                new ProducerRecord<>(PRODUCER_TEST_TOPIC, 2, "value2")))
        .toTopic("test1")
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

  @Configuration
  static class TestConfiguration {}
}
