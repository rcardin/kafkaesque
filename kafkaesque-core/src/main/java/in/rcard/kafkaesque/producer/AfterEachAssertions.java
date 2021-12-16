package in.rcard.kafkaesque.producer;

import in.rcard.kafkaesque.producer.KafkaesqueProducer.Record;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

class AfterEachAssertions<Key, Value> {
  
  private final KafkaesqueProducer<Key, Value> producer;
  private final List<Record<Key, Value>> records;
  private final Duration waitForConsumerDuration;
  
  AfterEachAssertions(
      KafkaesqueProducer<Key, Value> producer,
      List<Record<Key, Value>> records,
      Duration waitForConsumerDuration) {
    this.producer = producer;
    this.records = records;
    this.waitForConsumerDuration = waitForConsumerDuration;
  }
  
  /**
   * Asserts that some conditions hold on a single sent message.<br> For example:
   *
   * <pre>
   *   producer.asserting(pr ->
   *     assertThat(pr.key()).isEqualTo("key")
   *   );
   * </pre>
   *
   * @param messageConsumer The conditions that must hold on a message
   */
  void asserting(Consumer<ProducerRecord<Key, Value>> messageConsumer) {
    records.forEach(
        record -> {
          final ProducerRecord<Key, Value> producerRecord = producer.sendRecord(record);
          consume(messageConsumer, producerRecord);
        });
  }
  
  private void consume(
      Consumer<ProducerRecord<Key, Value>> messageConsumer, ProducerRecord<Key, Value> record) {
    try {
      Awaitility.await()
          .atMost(waitForConsumerDuration)
          .untilAsserted(() -> messageConsumer.accept(record));
    } catch (ConditionTimeoutException ex) {
      throw new AssertionError(
          String.format(
              "The consuming of the message %s takes more than %d milliseconds",
              record, waitForConsumerDuration.toMillis()));
    }
  }
}
