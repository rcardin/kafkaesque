package in.rcardin.kafkaesque.niu;

import in.rcardin.kafkaesque.niu.KafkaesqueConsumerNew.Builder;

public final class KafkaesqueNew {
  
  private final String brokersUrl;
  
  private KafkaesqueNew(String brokersUrl) {
    this.brokersUrl = brokersUrl;
  }
  
  <Key, Value> KafkaesqueConsumerNew.Builder<Key, Value> consume() {
    return Builder.newInstance(brokersUrl);
  }
//
//  <Key, Value> Builder<Key, Value> produce();
//
//  default <Key, Value> KafkaesqueInputTopic<Key, Value> createInputTopic(
//      String topic, Serializer<Key> keySerializer, Serializer<Value> valueSerializer) {
//    final Builder<Key, Value> builder =
//        this.<Key, Value>produce()
//            .toTopic(topic)
//            .withSerializers(keySerializer, valueSerializer);
//    return new KafkaesqueInputTopic<>(builder);
//  }
//
//  default <Key, Value> KafkaesqueOutputTopic<Key, Value> createOutputTopic(
//      String topic, Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer) {
//    final KafkaesqueConsumer.Builder<Key, Value> builder =
//        this.<Key, Value>consume()
//            .fromTopic(topic)
//            .withDeserializers(keyDeserializer, valueDeserializer)
//            .waitingAtMost(1L, TimeUnit.MINUTES)
//            .waitingEmptyPolls(10, 1000L, TimeUnit.MILLISECONDS);
//    return new KafkaesqueOutputTopic<>(builder);
//  }

  static KafkaesqueNew usingBroker(String brokersUrl) {
    return new KafkaesqueNew(brokersUrl);
  }
}
