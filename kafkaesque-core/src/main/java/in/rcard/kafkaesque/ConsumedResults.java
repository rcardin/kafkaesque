package in.rcard.kafkaesque;

import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumedResults<Key, Value> {
  
  private final Collection<ConsumerRecord<Key, Value>> consumerRecords;
  
  ConsumedResults(Collection<ConsumerRecord<Key, Value>> consumerRecords) {
    this.consumerRecords = consumerRecords;
  }
}
