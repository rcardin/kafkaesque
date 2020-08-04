package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;

import in.rcard.kafkaesque.KafkaesqueConsumer.Builder;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

// XXX There is a big deal with the code structured this way. We can test only the happy path :(
//     Try to rethink the Kafkaesque type to allow a complete testing strategy.
class KafkaesqueTest {
  
  @Test
  void consumeShouldReturnAnInstanceOfAConcreteBuilder() {
    final Builder<?> builder =
        Kafkaesque
            .newInstance("embeddedKafka")
            .consume();
    assertThat(builder)
        .isNotNull()
        .isInstanceOf(TestBuilder.class);
  }

  static class TestBuilder implements Builder<String> {
  
    public static TestBuilder newInstance() {
      return new TestBuilder();
    }
    
    @Override
    public Builder<String> fromTopic(String topic) {
      return null;
    }
  
    @Override
    public Builder<String> withDeserializer(Deserializer<String> deserializer) {
      return null;
    }
  
    @Override
    public Builder<String> waitingAtMost(long interval, TimeUnit unit) {
      return null;
    }
  
    @Override
    public KafkaesqueConsumer<String> expecting() {
      return null;
    }
  }
}