package in.rcard.kafkaesque.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import in.rcard.kafkaesque.producer.KafkaesqueProducer.Record;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class})
class KafkaesqueProducerDSLTest {

  private static final String TEST_TOPIC = "test-topic";
  
  private static final List<Record<String, String>> MESSAGES =
      Collections.singletonList(Record.of("key", "message"));

  private KafkaesqueProducerDSL<String, String> dsl;

  @BeforeEach
  void setUp() {
    dsl = KafkaesqueProducerDSL.newInstance(TEST_TOPIC);
  }
  
  @Test
  void withSerializersShouldReturnTheSameInstanceOfTheBuilder() {
    assertThat(dsl.withSerializers(new StringSerializer(), new StringSerializer()))
        .isEqualTo(dsl);
  }

  @Test
  void messagesShouldReturnTheSameInstanceOfTheBuilder() {
    assertThat(dsl.messages(Collections.emptyList())).isEqualTo(dsl);
  }

  @Test
  void waitingAtMostForEachAckShouldReturnTheSameInstanceOfTheBuilder() {
    assertThat(dsl.waitingAtMostForEachAck(42L, TimeUnit.SECONDS)).isEqualTo(dsl);
  }

  @Test
  void waitingForTheConsumerAtMostShouldReturnTheSameInstanceOfTheBuilder() {
    assertThat(dsl.waitingForTheConsumerAtMost(42L, TimeUnit.SECONDS)).isEqualTo(dsl);
  }

  @Test
  void andAfterAllShouldThrowAnIAEIfTheFunctionToCreateTheDelegateProducerIsNull() {
    assertThatThrownBy(() -> KafkaesqueProducerDSL.newInstance(null).andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The brokerUrl cannot be empty");
  }
  
  @Test
  void andAfterEachShouldThrowAnIAEIfTheFunctionToCreateTheDelegateProducerIsNull() {
    assertThatThrownBy(() -> KafkaesqueProducerDSL.newInstance(null).andAfterEach())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The brokerUrl cannot be empty");
  }

  @Test
  void andAfterAllShouldThrowAnIAEIfTheTopicIsNull() {
    assertThatThrownBy(() -> dsl.andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }

  @Test
  void andAfterAllShouldThrowAnIAEIfTheTopicIsEmpty() {
    assertThatThrownBy(() -> dsl.toTopic("").andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }

  @Test
  void andAfterAllShouldThrowAnIAEIfTheTopicIsBlank() {
    assertThatThrownBy(() -> dsl.toTopic("    ").andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }

  @Test
  void andAfterAllShouldThrowAnIAEIfTheListOfMessagesIsNull() {
    assertThatThrownBy(() -> dsl.toTopic("topic").andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The list of records to send cannot be empty");
  }

  @Test
  void andAfterAllShouldThrowAnIAEIfTheListOfMessagesIsEmpty() {
    assertThatThrownBy(() -> dsl.toTopic("topic").messages(Collections.emptyList()).andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The list of records to send cannot be empty");
  }

  @Test
  void andAfterAllShouldThrowAnIAEIfTheKeySerializerIsNull() {
    assertThatThrownBy(
            () ->
                dsl
                    .toTopic("topic")
                    .messages(MESSAGES)
                    .withSerializers(null, new StringSerializer())
                    .andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The serializers cannot be null");
  }

  @Test
  void andAfterAllShouldThrowAnIAEIfTheValueSerializerIsNull() {
    assertThatThrownBy(
            () ->
                dsl
                    .toTopic("topic")
                    .messages(MESSAGES)
                    .withSerializers(new StringSerializer(), null)
                    .andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The serializers cannot be null");
  }
  
  @Test
  void andAfterEachShouldThrowAnIAEIfTheTopicIsNull() {
    assertThatThrownBy(() -> dsl.andAfterEach())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }
  
  @Test
  void andAfterEachShouldThrowAnIAEIfTheTopicIsEmpty() {
    assertThatThrownBy(() -> dsl.toTopic("").andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }
  
  @Test
  void andAfterEachShouldThrowAnIAEIfTheTopicIsBlank() {
    assertThatThrownBy(() -> dsl.toTopic("    ").andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }
  
  @Test
  void andAfterEachShouldThrowAnIAEIfTheListOfMessagesIsNull() {
    assertThatThrownBy(() -> dsl.toTopic("topic").andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The list of records to send cannot be empty");
  }
  
  @Test
  void andAfterEachShouldThrowAnIAEIfTheListOfMessagesIsEmpty() {
    assertThatThrownBy(() -> dsl.toTopic("topic").messages(Collections.emptyList()).andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The list of records to send cannot be empty");
  }
  
  @Test
  void andAfterEachShouldThrowAnIAEIfTheKeySerializerIsNull() {
    assertThatThrownBy(
        () ->
            dsl
                .toTopic("topic")
                .messages(MESSAGES)
                .withSerializers(null, new StringSerializer())
                .andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The serializers cannot be null");
  }
  
  @Test
  void andAfterEachShouldThrowAnIAEIfTheValueSerializerIsNull() {
    assertThatThrownBy(
        () ->
            dsl
                .toTopic("topic")
                .messages(MESSAGES)
                .withSerializers(new StringSerializer(), null)
                .andAfterAll())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The serializers cannot be null");
  }
}
