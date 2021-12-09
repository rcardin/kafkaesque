package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.any;
import static org.mockito.BDDMockito.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

// XXX There is a big deal with the code structured this way. We can test only the happy path :(
//     Try to rethink the Kafkaesque type to allow a complete testing strategy.
class KafkaesqueTest {

  @Test
  void usingBrokerShouldReturnAnInstanceOfAConcreteKafkaesque() {
    final Kafkaesque kafkaesque = Kafkaesque.usingBroker("broker");
    assertThat(kafkaesque).isNotNull().isInstanceOf(TestKafkaesque.class);
  }

  @Test
  void createInputTopicShouldReturnAKafkaesqueInputTopic() {
    final KafkaesqueInputTopic<Integer, String> inputTopic =
        Kafkaesque.usingBroker("broker")
            .createInputTopic("input", new IntegerSerializer(), new StringSerializer());
    assertThat(inputTopic).isNotNull();
  }
  
  @Test
  void createOutputTopicShouldReturnAKafkaesqueOutputTopic() {
    final KafkaesqueOutputTopic<Integer, String> outputTopic =
        Kafkaesque.usingBroker("broker")
            .createOutputTopic("input", new IntegerDeserializer(), new StringDeserializer());
    assertThat(outputTopic).isNotNull();
  }

  private static class TestKafkaesque implements Kafkaesque {

    public TestKafkaesque(String broker) {
      // Empty body
    }

    @Override
    public <Key, Value> Builder<Key, Value> consume() {
      //noinspection unchecked
      final Builder<Key, Value> builder = mock(Builder.class);
      given(builder.fromTopic(anyString())).willReturn(builder);
      given(builder.withDeserializers(any(), any())).willReturn(builder);
      given(builder.waitingAtMost(anyLong(), any())).willReturn(builder);
      given(builder.waitingEmptyPolls(anyInt(), anyLong(), any())).willReturn(builder);
      return builder;
    }

    @Override
    public <Key, Value> KafkaesqueProducer.Builder<Key, Value> produce() {
      //noinspection unchecked
      final KafkaesqueProducer.Builder<Key, Value> builder =
          mock(KafkaesqueProducer.Builder.class);
      given(builder.toTopic(anyString())).willReturn(builder);
      given(builder.withSerializers(any(), any())).willReturn(builder);
      return builder;
    }
  }
}
