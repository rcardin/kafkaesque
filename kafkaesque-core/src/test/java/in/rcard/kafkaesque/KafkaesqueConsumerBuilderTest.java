package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import in.rcard.kafkaesque.KafkaesqueConsumer.Builder;
import in.rcard.kafkaesque.KafkaesqueConsumer.KafkaesqueConsumerDelegate;
import in.rcard.kafkaesque.KafkaesqueConsumer.KafkaesqueConsumerDelegate.DelegateCreationInfo;
import in.rcard.kafkaesque.KafkaesqueProducer.KafkaesqueProducerDelegate;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaesqueConsumerBuilderTest {

  @Mock
  private Function<
          DelegateCreationInfo<String, String>,
          ? extends KafkaesqueConsumerDelegate<String, String>>
      delegateCreator;
  @Mock
  private KafkaesqueConsumerDelegate<String, String> delegate;
  @Mock
  private ConsumerRecord<String, String> record;
  
  private Builder<String, String> builder;

  @BeforeEach
  void setUp() {
    builder = Builder.newInstance(delegateCreator);
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
    assertThat(builder.waitingEmptyPolls(2, 50L, TimeUnit.MILLISECONDS))
        .isEqualTo(builder);
  }

  @Test
  void expectingShouldThrowAnIAEIfTheFunctionToCreateTheDelegateConsumerIsNull() {
    assertThatThrownBy(() -> Builder.newInstance(null).expecting())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The function creating the consumer delegate cannot be null");
  }

  @Test
  void expectingShouldThrowAnIAEIfTheTopicIsNull() {
    assertThatThrownBy(() -> builder.expecting())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }

  @Test
  void expectingShouldThrowAnIAEIfTheTopicIsEmpty() {
    assertThatThrownBy(() -> builder.fromTopic("").expecting())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }

  @Test
  void expectingShouldThrowAnIAEIfTheKeyDeserializerIsNull() {
    assertThatThrownBy(() -> builder.fromTopic("topic").expecting())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The deserializers cannot be null");
  }

  @Test
  void expectingShouldThrowAnIAEIfTheValueDeserializerIsNull() {
    assertThatThrownBy(
            () ->
                builder
                    .fromTopic("topic")
                    .withDeserializers(new StringDeserializer(), null)
                    .expecting())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The deserializers cannot be null");
  }

  @Test
  void expectingShouldReturnANewInstanceOfAKafkaesqueConsumer() {
    //noinspection unchecked
    given(delegateCreator.apply(any(DelegateCreationInfo.class))).willReturn(delegate);
    given(delegate.poll())
        .willReturn(Collections.singletonList(record))
        .willReturn(Collections.emptyList());
    final ConsumedResultsAndKafkaesqueConsumerDelegate<String, String> consumer =
        builder
            .withDeserializers(new StringDeserializer(), new StringDeserializer())
            .fromTopic("topic")
            .waitingAtMost(1L, TimeUnit.SECONDS)
            .expecting();
    assertThat(consumer).isNotNull();
  }
}
