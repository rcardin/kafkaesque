package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import in.rcard.kafkaesque.KafkaesqueProducer.Builder;
import in.rcard.kafkaesque.KafkaesqueProducer.KafkaesqueProducerDelegate;
import in.rcard.kafkaesque.KafkaesqueProducer.KafkaesqueProducerDelegate.DelegateCreationInfo;
import java.util.function.Function;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class})
class KafkaesqueProducerBuilderTest {
  
  @Mock
  private Function<DelegateCreationInfo<String, String>, KafkaesqueProducerDelegate<String, String>>
      delegateCreator;
  
  private Builder<String, String> builder;

  @BeforeEach
  void setUp() {
    builder = Builder.newInstance(delegateCreator);
  }

  @Test
  void expectingShouldThrowAnIAEIfTheFunctionToCreateTheDelegateProducerIsNull() {
    assertThatThrownBy(() -> Builder.newInstance(null).expecting())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The function creating the producer delegate cannot be null");
  }

  @Test
  void expectingShouldThrowAnIAEIfTheTopicIsNull() {
    assertThatThrownBy(() -> builder.expecting())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }
  
  @Test
  void expectingShouldThrowAnIAEIfTheTopicIsEmpty() {
    assertThatThrownBy(() -> builder.toTopic("").expecting())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }
  
  @Test
  void expectingShouldThrowAnIAEIfTheTopicIsBlank() {
    assertThatThrownBy(() -> builder.toTopic("    ").expecting())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic name cannot be empty");
  }
}