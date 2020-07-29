package in.rcard.kafkaesque;

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Deserializer;

public class Kafkaesque {
  
  private Kafkaesque() {
    // Empty body
  }
  
  private static Kafkaesque newInstance() {
    return new Kafkaesque();
  }
  
  private <T> KafkaesqueConsumer<T> withConsumer(String topic, Deserializer<T> deserializer) {
    return new KafkaesqueConsumer<T>(topic, deserializer);
  }
  
  static class KafkaesqueConsumer<T> {
    private final String topic;
    private final Deserializer<T> deserializer;
  
    KafkaesqueConsumer(String topic,
        Deserializer<T> deserializer) {
      validateTopic(topic);
      validateDeserializer(deserializer);
      this.topic = topic;
      this.deserializer = deserializer;
    }
  
    private void validateDeserializer(Deserializer<T> deserializer) {
      if (deserializer == null) {
        throw new IllegalArgumentException("The deserializer cannot be null");
      }
    }
  
    private void validateTopic(String topic) {
      if (StringUtils.isEmpty(topic)) {
        throw new IllegalArgumentException("The topic cannot be empty");
      }
    }
  
    public KafkaesqueConsumer<T> waitingAtMost(long interval, TimeUnit unit) {
      // TODO
      return this;
    }
  }
}
