import in.rcard.kafkaesque.ConsumedResults;
import in.rcard.kafkaesque.KafkaesqueConsumer;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.util.StringUtils;

public class SpringKafkaesqueConsumer<T> implements KafkaesqueConsumer<T> {
  
  public ConsumedResults<T> poll() {
    return null;
  }
  
  public static class Builder<T> implements KafkaesqueConsumer.Builder<T> {
  
    private String topic;
    private Deserializer<T> deserializer;
    private long interval = 200;
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    
    private Builder() {}
    
    public static <T> Builder<T> newInstance() {
      return new Builder<T>();
    }
    
    public KafkaesqueConsumer.Builder<T> fromTopic(String topic) {
      this.topic = topic;
      return this;
    }
  
    public KafkaesqueConsumer.Builder<T> withDeserializer(Deserializer<T> deserializer) {
      this.deserializer = deserializer;
      return this;
    }
  
    public KafkaesqueConsumer.Builder<T> waitingAtMost(long interval, TimeUnit unit) {
      this.interval = interval;
      this.timeUnit = unit;
      return this;
    }
  
    public SpringKafkaesqueConsumer<T> expecting() {
      validateInputs();
      return null;
    }
  
    private void validateInputs() {
      validateTopic();
      validateDeserializer();
    }
  
    private void validateTopic() {
      if (StringUtils.isEmpty(topic)) {
        throw new AssertionError("The topic name cannot be empty");
      }
    }
  
    private void validateDeserializer() {
      if (deserializer == null) {
        throw new AssertionError("The deserializer cannot be null");
      }
    }
  }
}
