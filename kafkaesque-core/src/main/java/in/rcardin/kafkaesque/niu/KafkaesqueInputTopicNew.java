package in.rcardin.kafkaesque.niu;

import in.rcardin.kafkaesque.niu.KafkaesqueProducerNew.Builder;
import java.util.List;

public class KafkaesqueInputTopicNew<Key, Value> {

  private final Builder<Key, Value> producerBuilder;
  
  KafkaesqueInputTopicNew(
      Builder<Key, Value> producerBuilder) {
    this.producerBuilder = producerBuilder;
  }
  
  public void pipeRecordList(List<KafkaesqueProducerNew.Record<Key, Value>> records) {
    producerBuilder
        .messages(records)
        .expecting()
        .assertingAfterAll(producerRecords -> {});
  }
}
