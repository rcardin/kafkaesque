package in.rcard.kafkaesque;

import in.rcard.kafkaesque.KafkaesqueConsumer.Builder;

public class KafkaesqueOutputTopic<Key, Value> {

  private final Builder<Key, Value> consumerBuilder;
  
  KafkaesqueOutputTopic(
      Builder<Key, Value> consumerBuilder) {
    this.consumerBuilder = consumerBuilder;
  }
  
  
}
