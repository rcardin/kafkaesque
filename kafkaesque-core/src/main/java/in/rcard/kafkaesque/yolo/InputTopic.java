package in.rcard.kafkaesque.yolo;

import in.rcard.kafkaesque.producer.KafkaesqueProducerDSL;
import in.rcard.kafkaesque.producer.KafkaesqueProducer.Record;
import java.util.List;

public class InputTopic<Key, Value> {

  private final KafkaesqueProducerDSL<Key, Value> dsl;

  InputTopic(KafkaesqueProducerDSL<Key, Value> dsl) {
    this.dsl = dsl;
  }

  public void pipeRecords(Record<Key, Value>... records) {
    pipeRecordsList(List.of(records));
  }

  public void pipeRecordsList(List<Record<Key, Value>> records) {
    dsl.messages(records).andAfterAll().asserting(producerRecords -> {});
  }
}
