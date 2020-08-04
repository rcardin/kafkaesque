package in.rcard.kafkaesque;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;

public interface KafkaesqueConsumer<T> {
  interface Builder<T> {
    Builder<T> fromTopic(String topic);
    Builder<T> withDeserializer(Deserializer<T> deserializer);
    Builder<T> waitingAtMost(long interval, TimeUnit unit);
    
    KafkaesqueConsumer<T> expecting();
  }
  
  ConsumedResults<T> poll();
}
