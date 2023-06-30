package in.rcard.kafkaesque.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaesqueConsumerDSLTest {

  private KafkaesqueConsumerDSL<String, String> builder;

  @BeforeEach
  void setUp() {
    builder = KafkaesqueConsumerDSL.newInstance("kafka:9092");
  }

  @Test
  void fromTopicShouldReturnTheSameInstanceOfTheBuilder() {
    assertThat(builder.fromTopic("topic")).isEqualTo(builder);
  }

  @Test
  void withDeserializersShouldReturnTheSameInstanceOfTheBuilder() {
    assertThat(builder.withDeserializers(new StringDeserializer(), new StringDeserializer()))
        .isEqualTo(builder);
  }

  @Test
  void waitingAtMostShouldReturnTheSameInstanceOfTheBuilder() {
    assertThat(builder.waitingAtMost(100L, TimeUnit.MILLISECONDS)).isEqualTo(builder);
  }

  @Test
  void waitingEmptyPollsShouldReturnTheSameInstanceOfTheBuilder() {
    assertThat(builder.waitingEmptyPolls(2, 50L, TimeUnit.MILLISECONDS)).isEqualTo(builder);
  }

  @Test
  void withConfigurationShouldReturnTheSameInstanceOfTheBuilder() {
    assertThat(builder.withConfiguration("value")).isEqualTo(builder);
  }

  @Test
  void expectingConsumedShouldThrowAnIAEIfTheBrokerUrlIsNull() {
    assertThatThrownBy(() -> KafkaesqueConsumerDSL.newInstance(null).expectingConsumed())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The broker url cannot be empty");
  }

  @Test
  void expectingConsumedShouldThrowAnIAEIfTheTopicIsNull() {
    assertThatThrownBy(() -> builder.expectingConsumed())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }

  @Test
  void expectingConsumedShouldThrowAnIAEIfTheTopicIsEmpty() {
    assertThatThrownBy(() -> builder.fromTopic("").expectingConsumed())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }

  @Test
  void expectingConsumedShouldThrowAnIAEIfTheKeyDeserializerIsNull() {
    assertThatThrownBy(() -> builder.fromTopic("topic").expectingConsumed())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The deserializers cannot be null");
  }

  @Test
  void expectingConsumedShouldThrowAnIAEIfTheValueDeserializerIsNull() {
    assertThatThrownBy(
            () ->
                builder
                    .fromTopic("topic")
                    .withDeserializers(new StringDeserializer(), null)
                    .expectingConsumed())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The deserializers cannot be null");
  }

  @Test
  void expectingConsumedShouldThrowAnIAEIfTheGivenConfigurationFilePathDoesNotExist() {
    assertThatThrownBy(
            () ->
                builder
                    .fromTopic("topic")
                    .withDeserializers(new StringDeserializer(), new StringDeserializer())
                    .withConfiguration("not-existing-file.properties")
                    .expectingConsumed())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The configuration file 'not-existing-file.properties' does not exist");
  }
}
