package in.rcard.kafkaesque;

import in.rcard.kafkaesque.KafkaesqueProducer.Builder;
import java.util.List;

public class KafkaesqueInputTopic<Key, Value> {

  private final Builder<Key, Value> producerBuilder;
  
  KafkaesqueInputTopic(
      Builder<Key, Value> producerBuilder) {
    this.producerBuilder = producerBuilder;
  }
  
  public void pipeRecordList(List<KafkaesqueProducer.Record<Key, Value>> records) {
    producerBuilder
        .messages(records)
        .expecting()
        .assertingAfterAll(producerRecords -> {});
  }
}
