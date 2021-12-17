package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import in.rcard.kafkaesque.producer.KafkaesqueProducer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class})
class KafkaesqueProducerBuilderTest {

//  private static final List<Record<String, String>> MESSAGES =
//      Collections.singletonList(Record.of("key", "message"));
//
//  @Mock
//  private Function<DelegateCreationInfo<String, String>, KafkaesqueProducerDelegate<String, String>>
//      delegateCreator;
//
//  private Builder<String, String> builder;
//
//  @BeforeEach
//  void setUp() {
//    builder = Builder.newInstance(delegateCreator);
//  }
//
//  @Test
//  void toTopicShouldReturnTheSameInstanceOfTheBuilder() {
//    assertThat(builder.toTopic("topic")).isEqualTo(builder);
//  }
//
//  @Test
//  void withSerializersShouldReturnTheSameInstanceOfTheBuilder() {
//    assertThat(builder.withSerializers(new StringSerializer(), new StringSerializer()))
//        .isEqualTo(builder);
//  }
//
//  @Test
//  void messagesShouldReturnTheSameInstanceOfTheBuilder() {
//    assertThat(builder.messages(Collections.emptyList())).isEqualTo(builder);
//  }
//
//  @Test
//  void waitingAtMostForEachAckShouldReturnTheSameInstanceOfTheBuilder() {
//    assertThat(builder.waitingAtMostForEachAck(42L, TimeUnit.SECONDS)).isEqualTo(builder);
//  }
//
//  @Test
//  void waitingForTheConsumerAtMostShouldReturnTheSameInstanceOfTheBuilder() {
//    assertThat(builder.waitingForTheConsumerAtMost(42L, TimeUnit.SECONDS)).isEqualTo(builder);
//  }
//
//  @Test
//  void expectingShouldThrowAnIAEIfTheFunctionToCreateTheDelegateProducerIsNull() {
//    assertThatThrownBy(() -> Builder.newInstance(null).expecting())
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("The function creating the producer delegate cannot be null");
//  }
//
//  @Test
//  void expectingShouldThrowAnIAEIfTheTopicIsNull() {
//    assertThatThrownBy(() -> builder.expecting())
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("The topic name cannot be empty");
//  }
//
//  @Test
//  void expectingShouldThrowAnIAEIfTheTopicIsEmpty() {
//    assertThatThrownBy(() -> builder.toTopic("").expecting())
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("The topic name cannot be empty");
//  }
//
//  @Test
//  void expectingShouldThrowAnIAEIfTheTopicIsBlank() {
//    assertThatThrownBy(() -> builder.toTopic("    ").expecting())
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("The topic name cannot be empty");
//  }
//
//  @Test
//  void expectingShouldThrowAnIAEIfTheListOfMessagesIsNull() {
//    assertThatThrownBy(() -> builder.toTopic("topic").expecting())
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("The list of records to send cannot be empty");
//  }
//
//  @Test
//  void expectingShouldThrowAnIAEIfTheListOfMessagesIsEmpty() {
//    assertThatThrownBy(() -> builder.toTopic("topic").messages(Collections.emptyList()).expecting())
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("The list of records to send cannot be empty");
//  }
//
//  @Test
//  void expectingShouldThrowAnIAEIfTheKeySerializerIsNull() {
//    assertThatThrownBy(
//            () ->
//                builder
//                    .toTopic("topic")
//                    .messages(MESSAGES)
//                    .withSerializers(null, new StringSerializer())
//                    .expecting())
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("The serializers cannot be null");
//  }
//
//  @Test
//  void expectingShouldThrowAnIAEIfTheValueSerializerIsNull() {
//    assertThatThrownBy(
//            () ->
//                builder
//                    .toTopic("topic")
//                    .messages(MESSAGES)
//                    .withSerializers(new StringSerializer(), null)
//                    .expecting())
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("The serializers cannot be null");
//  }
//
//  @Test
//  void expectingShouldReturnANewInstanceOfAKafkaesqueProducer() {
//    final KafkaesqueProducer<String, String> producer =
//        builder
//            .toTopic("topic")
//            .withSerializers(new StringSerializer(), new StringSerializer())
//            .messages(MESSAGES)
//            .expecting();
//    assertThat(producer).isNotNull();
//  }
}
