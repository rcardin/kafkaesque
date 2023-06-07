package in.rcard.kafkaesque.producer;

import in.rcard.kafkaesque.producer.KafkaesqueProducer.Record;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

public class AfterAllAssertions<Key, Value> {
  
  private final KafkaesqueProducer<Key, Value> producer;
  private final List<Record<Key, Value>> records;
  private final Duration waitForConsumerDuration;
  
  AfterAllAssertions(
      KafkaesqueProducer<Key, Value> producer,
      List<Record<Key, Value>> records,
      Duration waitForConsumerDuration) {
    this.producer = producer;
    this.records = records;
    this.waitForConsumerDuration = waitForConsumerDuration;
  }
  
  /**
   * Asserts that some conditions hold on the whole list of sent message.<br> For example:
   *
   * <pre>
   *   producer.asserting(records -&gt;
   *     assertThat(records).hasSize(2)
   *   );
   * </pre>
   *
   * @param messagesConsumer The conditions that must hold on the whole list of messages
   */
  public void asserting(Consumer<List<ProducerRecord<Key, Value>>> messagesConsumer) {
    final List<ProducerRecord<Key, Value>> producerRecords = producer.sendRecords(records);
    consume(messagesConsumer, producerRecords);
  }
  
  private void consume(
      Consumer<List<ProducerRecord<Key, Value>>> messagesConsumer,
      List<ProducerRecord<Key, Value>> records) {
    try {
      Awaitility.await()
          .atMost(waitForConsumerDuration)
          .untilAsserted(() -> messagesConsumer.accept(records));
    } catch (ConditionTimeoutException ex) {
      handleConditionTimeoutException(records, ex);
    }
  }
  
  private void handleConditionTimeoutException(
      List<ProducerRecord<Key, Value>> records,
      ConditionTimeoutException ex) {
    if (ex.getCause() instanceof AssertionError) {
      throw (AssertionError) ex.getCause();
    }
    throw new AssertionError(
        String.format(
            "Consuming the list of %s messages takes more than %d milliseconds",
            records, waitForConsumerDuration.toMillis()));
  }
}
