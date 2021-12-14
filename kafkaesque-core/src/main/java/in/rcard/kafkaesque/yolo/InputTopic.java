package in.rcard.kafkaesque.yolo;

import in.rcard.kafkaesque.producer.KafkaesqueProducerDSL;
import in.rcard.kafkaesque.producer.KafkaesqueProducer.Record;
import java.util.List;

public class InputTopic<Key, Value> {

  private final KafkaesqueProducerDSL<Key, Value> producerBuilder;
  
  InputTopic(
      KafkaesqueProducerDSL<Key, Value> producerBuilder) {
    this.producerBuilder = producerBuilder;
  }
  
  public void pipeRecordList(List<Record<Key, Value>> records) {
    producerBuilder
        .messages(records)
        .expecting()
        .assertingAfterAll(producerRecords -> {});
  }
}
