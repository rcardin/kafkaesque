package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;

import in.rcard.kafkaesque.KafkaesqueConsumer.Builder;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

class KafkaesqueTest {
  
  @Test
  void consumeShouldReturnAnInstanceOfAConcreteBuilder() {
    final Builder builder =
        Kafkaesque
            .newInstance()
            .consume();
    assertThat(builder)
        .isNotNull()
        .isInstanceOf(TestBuilder.class);
  }

  static class TestBuilder implements Builder {
  
    public static TestBuilder newInstance() {
      return new TestBuilder();
    }
    
    @Override
    public Builder fromTopic(String topic) {
      return null;
    }
  
    @Override
    public Builder withDeserializer(Deserializer<?> deserializer) {
      return null;
    }
  
    @Override
    public Builder waitingAtMost(long interval, TimeUnit unit) {
      return null;
    }
  
    @Override
    public KafkaesqueConsumer<?> expecting() {
      return null;
    }
  }
}