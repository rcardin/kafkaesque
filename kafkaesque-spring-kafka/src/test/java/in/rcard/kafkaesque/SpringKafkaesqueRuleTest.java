package in.rcard.kafkaesque;

import in.rcard.kafkaesque.SpringKafkaesqueRuleTest.TestConfiguration;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@SpringBootTest(classes = {TestConfiguration.class})
public class SpringKafkaesqueRuleTest {

  private static final String CONSUMER_TEST_TOPIC = "test";

  @ClassRule
  public static final SpringKafkaesqueRule kafkaesqueRule =
      new SpringKafkaesqueRule(1, false, CONSUMER_TEST_TOPIC);

  private KafkaTemplate<Integer, String> kafkaTemplate;

  @Before
  public void setUp() {
    setUpProducer();
  }

  private void setUpProducer() {
    final DefaultKafkaProducerFactory<Integer, String> producerFactory =
        new DefaultKafkaProducerFactory<>(
            KafkaTestUtils.producerProps(kafkaesqueRule.getEmbeddedKafka()));
    kafkaTemplate = new KafkaTemplate<>(producerFactory);
    kafkaTemplate.setDefaultTopic(CONSUMER_TEST_TOPIC);
  }
  
  @Test
  public void theKafkaesqueRuleShouldInitializeTheConfigurationObjectProperly() {
    kafkaTemplate.sendDefault(1, "data1");
    kafkaTemplate.sendDefault(2, "data2");
    kafkaesqueRule.getKafkaesque()
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
  
  @Configuration
  static class TestConfiguration {}
}
