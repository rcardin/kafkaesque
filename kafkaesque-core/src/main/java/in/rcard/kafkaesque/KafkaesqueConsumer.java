package in.rcard.kafkaesque;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;

public interface KafkaesqueConsumer<T> {
  interface Builder {
    Builder fromTopic(String topic);
    Builder withDeserializer(Deserializer<?> deserializer);
    Builder waitingAtMost(long interval, TimeUnit unit);
    
    KafkaesqueConsumer<?> expecting();
  }
  
  ConsumedResults<T> poll();
}
