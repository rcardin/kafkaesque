package in.rcard.kafkaesque;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;

public interface KafkaesqueProducer<Key, Value> {
  interface Builder<K, Key, Value> {
    Builder<K, Key, Value> toTopic(String topic);
    Builder<K, Key, Value> withDeserializers(
        Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer);
    Builder<K, Key, Value> messages(List<ProducerRecord<Key, Value>> records);
    Builder<K, Key, Value> waitingAtMostForEachAck(long interval, TimeUnit unit);
    Builder<K, Key, Value> waitingForTheConsumerAtMost(long interval, TimeUnit unit);
    KafkaesqueProducer<Key, Value> expecting();
  }
}
