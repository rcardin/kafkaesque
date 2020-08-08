package in.rcard.kafkaesque;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;

public interface KafkaesqueConsumer<Key, Value> {
  interface Builder<K, Key, Value> {
    Builder<K, Key, Value> fromTopic(String topic);
    Builder<K, Key, Value> withDeserializers(Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer);
    Builder<K, Key, Value> waitingAtMost(long interval, TimeUnit unit);
    
    KafkaesqueConsumer<Key, Value> expecting();
  }
  
  ConsumedResults<Key, Value> poll();
}
