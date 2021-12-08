package in.rcardin.kafkaesque.niu;

public final class KafkaesqueNew {
  
  private final String brokersUrl;
  
  private KafkaesqueNew(String brokersUrl) {
    this.brokersUrl = brokersUrl;
  }
  
  <Key, Value> KafkaesqueConsumerNew.Builder<Key, Value> consume() {
    return KafkaesqueConsumerNew.Builder.newInstance(brokersUrl);
  }

  <Key, Value> KafkaesqueProducerNew.Builder<Key, Value> produce() {
    return KafkaesqueProducerNew.Builder.newInstance(brokersUrl);
  }
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
