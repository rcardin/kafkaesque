package in.rcardin.kafkaesque.niu;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public final class KafkaesqueNew {
  
  private final String brokersUrl;
  
  private KafkaesqueNew(String brokersUrl) {
    this.brokersUrl = brokersUrl;
  }
  
  public <Key, Value> KafkaesqueConsumerNew.Builder<Key, Value> consume() {
    return KafkaesqueConsumerNew.Builder.newInstance(brokersUrl);
  }

  public <Key, Value> KafkaesqueProducerNew.Builder<Key, Value> produce() {
    return KafkaesqueProducerNew.Builder.newInstance(brokersUrl);
  }

  public <Key, Value> KafkaesqueInputTopicNew<Key, Value> createInputTopic(
      String topic, Serializer<Key> keySerializer, Serializer<Value> valueSerializer) {
    final KafkaesqueProducerNew.Builder<Key, Value> builder =
        this.<Key, Value>produce()
            .toTopic(topic)
            .withSerializers(keySerializer, valueSerializer);
    return new KafkaesqueInputTopicNew<>(builder);
  }

  public <Key, Value> KafkaesqueOutputTopicNew<Key, Value> createOutputTopic(
      String topic, Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer) {
    final KafkaesqueConsumerNew.Builder<Key, Value> builder =
        this.<Key, Value>consume()
            .fromTopic(topic)
            .withDeserializers(keyDeserializer, valueDeserializer)
            .waitingAtMost(1L, TimeUnit.MINUTES)
            .waitingEmptyPolls(10, 1000L, TimeUnit.MILLISECONDS);
    return new KafkaesqueOutputTopicNew<>(builder);
  }

  static KafkaesqueNew usingBroker(String brokersUrl) {
    return new KafkaesqueNew(brokersUrl);
  }
}
