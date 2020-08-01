package in.rcard.kafkaesque;

import in.rcard.kafkaesque.KafkaesqueConsumer.Builder;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

class KafkaesqueTest {

  @Test
  void consumeShouldReturnAnInstanceOfAConcreteBuilder() {
    Kafkaesque.newInstance().consume();
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