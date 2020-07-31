package in.rcard.kafkaesque;

import org.apache.kafka.common.serialization.Deserializer;

public class Kafkaesque {
  
  private Kafkaesque() {
    // Empty body
  }
  
  private static Kafkaesque newInstance() {
    return new Kafkaesque();
  }
  
  private <T> KafkaesqueConsumer.Builder<T> consume() {
    return KafkaesqueConsumer.Builder.newInstance();
  }
  
}
