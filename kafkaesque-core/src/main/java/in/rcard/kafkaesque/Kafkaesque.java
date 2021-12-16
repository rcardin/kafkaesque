package in.rcard.kafkaesque;

import in.rcard.kafkaesque.consumer.KafkaesqueConsumerDSL;
import in.rcard.kafkaesque.producer.KafkaesqueProducerDSL;

public final class Kafkaesque {
  
  private final String brokersUrl;
  
  public static Kafkaesque at(String brokersUrl) {
    validateBrokersUrl(brokersUrl);
    return new Kafkaesque(brokersUrl);
  }
  
  private static void validateBrokersUrl(String brokersUrl) {
    if (brokersUrl == null || brokersUrl.isEmpty()) {
      throw new IllegalArgumentException("Brokers URL cannot be empty");
    }
  }
  
  private Kafkaesque(String brokersUrl) {
    this.brokersUrl = brokersUrl;
  }
  
  public <Key, Value> KafkaesqueConsumerDSL<Key, Value> consume() {
    return KafkaesqueConsumerDSL.newInstance(brokersUrl);
  }

  public <Key, Value> KafkaesqueProducerDSL<Key, Value> produce() {
    return KafkaesqueProducerDSL.newInstance(brokersUrl);
  }
}
