package in.rcard.kafkaesque;

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaesqueConsumer<T> {
  private final String topic;
  private final Deserializer<T> deserializer;

  KafkaesqueConsumer(String topic,
      Deserializer<T> deserializer) {
    this.topic = topic;
    this.deserializer = deserializer;
  }
  
  static class Builder<T> {
    private String topic;
    private Deserializer<T> deserializer;
    private long interval = 200L;
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    
    static <T> Builder<T> newInstance() {
      return new Builder<T>();
    }
  
    public Builder<T> fromTopic(String topic) {
      this.topic = topic;
      return this;
    }
    
    public Builder<T> withDeserializer(Deserializer<T> deserializer) {
      this.deserializer = deserializer;
      return this;
    }
    
    public Builder<T> waitingAtMost(long interval, TimeUnit unit) {
      this.interval = interval;
      this.timeUnit = unit;
      return this;
    }
    
    KafkaesqueConsumer<T> expecting() {
      validateTopic(topic);
      validateDeserializer(deserializer);
      return new KafkaesqueConsumer<T>(topic, deserializer);
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
  }
}
