package in.rcard.kafkaesque;

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
  
  public <Key, Value> KafkaesqueConsumer.Builder<Key, Value> consume() {
    return KafkaesqueConsumer.Builder.newInstance(brokersUrl);
  }

  public <Key, Value> KafkaesqueProducer.Builder<Key, Value> produce() {
    return KafkaesqueProducer.Builder.newInstance(brokersUrl);
  }
}
