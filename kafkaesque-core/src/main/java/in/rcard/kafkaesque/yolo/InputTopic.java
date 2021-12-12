package in.rcard.kafkaesque.yolo;

import in.rcard.kafkaesque.KafkaesqueProducer.Builder;
import in.rcard.kafkaesque.KafkaesqueProducer.Record;
import java.util.List;

public class InputTopic<Key, Value> {

  private final Builder<Key, Value> producerBuilder;
  
  InputTopic(
      Builder<Key, Value> producerBuilder) {
    this.producerBuilder = producerBuilder;
  }
  
  public void pipeRecordList(List<Record<Key, Value>> records) {
    producerBuilder
        .messages(records)
        .expecting()
        .assertingAfterAll(producerRecords -> {});
  }
}
