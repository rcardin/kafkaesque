package in.rcard.kafkaesque;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public final class Kafkaesque {
  
  private final String brokersUrl;
  
  private Kafkaesque(String brokersUrl) {
    this.brokersUrl = brokersUrl;
  }
  
  public <Key, Value> KafkaesqueConsumer.Builder<Key, Value> consume() {
    return KafkaesqueConsumer.Builder.newInstance(brokersUrl);
  }

  public <Key, Value> KafkaesqueProducer.Builder<Key, Value> produce() {
    return KafkaesqueProducer.Builder.newInstance(brokersUrl);
  }

  public <Key, Value> KafkaesqueInputTopic<Key, Value> createInputTopic(
      String topic, Serializer<Key> keySerializer, Serializer<Value> valueSerializer) {
    final KafkaesqueProducer.Builder<Key, Value> builder =
        this.<Key, Value>produce()
            .toTopic(topic)
            .withSerializers(keySerializer, valueSerializer);
    return new KafkaesqueInputTopic<>(builder);
  }

  public <Key, Value> KafkaesqueOutputTopic<Key, Value> createOutputTopic(
      String topic, Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer) {
    final KafkaesqueConsumer.Builder<Key, Value> builder =
        this.<Key, Value>consume()
            .fromTopic(topic)
            .withDeserializers(keyDeserializer, valueDeserializer)
            .waitingAtMost(1L, TimeUnit.MINUTES)
            .waitingEmptyPolls(10, 1000L, TimeUnit.MILLISECONDS);
    return new KafkaesqueOutputTopic<>(builder);
  }

  static Kafkaesque usingBroker(String brokersUrl) {
    return new Kafkaesque(brokersUrl);
  }
}
