import in.rcard.kafkaesque.ConsumedResults;
import in.rcard.kafkaesque.KafkaesqueConsumer;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.util.StringUtils;

public class SpringKafkaesqueConsumer<Key, Value> implements KafkaesqueConsumer<Key, Value> {
  
  public ConsumedResults<Key, Value> poll() {
    return null;
  }
  
  public static class Builder<Key, Value> implements KafkaesqueConsumer.Builder<EmbeddedKafkaBroker, Key, Value> {
  
    private String topic;
    private Deserializer<Key> keyDeserializer;
    private Deserializer<Value> valueDeserializer;
    private long interval = 200;
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    
    private Builder() {}
    
    public static <Key, Value> Builder<Key, Value> newInstance() {
      return new Builder<>();
    }
    
    public Builder<Key, Value> fromTopic(String topic) {
      this.topic = topic;
      return this;
    }
  
    public Builder<Key, Value> withDeserializers(
        Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer) {
      this.keyDeserializer = keyDeserializer;
      this.valueDeserializer = valueDeserializer;
      return this;
    }
  
    public Builder<Key, Value> waitingAtMost(long interval, TimeUnit unit) {
      this.interval = interval;
      this.timeUnit = unit;
      return this;
    }
  
    public SpringKafkaesqueConsumer<Key, Value> expecting() {
      validateInputs();
      return null;
    }
  
    private void validateInputs() {
      validateTopic();
      validateDeserializers();
    }
  
    private void validateTopic() {
      if (StringUtils.isEmpty(topic)) {
        throw new AssertionError("The topic name cannot be empty");
      }
    }
  
    private void validateDeserializers() {
      if (keyDeserializer == null || valueDeserializer == null) {
        throw new AssertionError("The deserializers cannot be null");
      }
    }
  }
}
