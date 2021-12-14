package in.rcard.kafkaesque.yolo;

import in.rcard.kafkaesque.Kafkaesque;
import in.rcard.kafkaesque.KafkaesqueConsumer;
import in.rcard.kafkaesque.producer.KafkaesqueProducerDSL;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public final class Kfksq {
  
  private final Kafkaesque kafkaesque;
  
  public static Kfksq at(String brokerUrl) {
    return new Kfksq(brokerUrl);
  }
  
  private Kfksq(String brokerUrl) {
    this.kafkaesque = Kafkaesque.at(brokerUrl);
  }
  
  public <Key, Value> InputTopic<Key, Value> createInputTopic(
      String topic, Serializer<Key> keySerializer, Serializer<Value> valueSerializer) {
    final KafkaesqueProducerDSL<Key, Value> builder =
        kafkaesque.<Key, Value>produce()
            .toTopic(topic)
            .withSerializers(keySerializer, valueSerializer);
    return new InputTopic<>(builder);
  }
  
  public <Key, Value> OutputTopic<Key, Value> createOutputTopic(
      String topic, Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer) {
    final KafkaesqueConsumer.Builder<Key, Value> builder =
        kafkaesque.<Key, Value>consume()
            .fromTopic(topic)
            .withDeserializers(keyDeserializer, valueDeserializer)
            .waitingAtMost(1L, TimeUnit.MINUTES)
            .waitingEmptyPolls(10, 1000L, TimeUnit.MILLISECONDS);
    return new OutputTopic<>(builder);
  }
}
