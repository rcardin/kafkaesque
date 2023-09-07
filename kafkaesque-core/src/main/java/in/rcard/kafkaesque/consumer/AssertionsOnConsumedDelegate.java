package in.rcard.kafkaesque.consumer;

import java.util.List;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.hamcrest.Matcher;

/**
 * A delegate type that allows the access to method both of the type {@link KafkaesqueConsumer}, and
 * the type {@link AssertionsOnConsumed}.
 *
 * @param <Key> The type of the messages' keys
 * @param <Value> The type of the messages' values
 *
 * @see AssertionsOnConsumed
 * @see KafkaesqueConsumer
 */
public class AssertionsOnConsumedDelegate<Key, Value> {
  private final AssertionsOnConsumed<Key, Value> results;
  private final KafkaesqueConsumer<Key, Value> consumer;

  AssertionsOnConsumedDelegate(
      AssertionsOnConsumed<Key, Value> results, KafkaesqueConsumer<Key, Value> consumer) {
    this.results = results;
    this.consumer = consumer;
  }
  
  /**
   * @see KafkaesqueConsumer#close()
   */
  public void andCloseConsumer() {
    consumer.close();
  }

  /**
   * Tests if the number of consumed messages is equal to the given {@code size}.
   *
   * @param size The expected number of messages
   * @return This instance
   *
   * @see AssertionsOnConsumed#havingRecordsSize(long)
   */
  public AssertionsOnConsumedDelegate<Key, Value> havingRecordsSize(long size) {
    results.havingRecordsSize(size);
    return this;
  }

  /**
   * Evaluates the list of headers to satisfy the given properties. Any kind of testing framework
   * can be used inside {@code headersConsumer}.
   *
   * @param headersConsumer Code testing the desired properties
   * @return This instance
   *
   * @see AssertionsOnConsumed#havingHeaders(Consumer)
   */
  public AssertionsOnConsumedDelegate<Key, Value> havingHeaders(
      Consumer<List<Headers>> headersConsumer) {
    results.havingHeaders(headersConsumer);
    return this;
  }

  /**
   * Evaluates the list of keys to satisfy the given properties. Any kind of testing framework can
   * be used inside {@code keysConsumer}.
   * @param keysConsumer Code testing the desired properties
   * @return This instance
   *
   * @see AssertionsOnConsumed#havingKeys(Consumer)
   */
  public AssertionsOnConsumedDelegate<Key, Value> havingKeys(Consumer<List<Key>> keysConsumer) {
    results.havingKeys(keysConsumer);
    return this;
  }

  /**
   * Evaluates the list of values to satisfy the given properties. Any kind of testing framework can
   * be used inside {@code valuesConsumer}.
   * @param valuesConsumer Code testing the desired properties
   * @return This instance
   *
   * @see AssertionsOnConsumed#havingPayloads(Consumer)
   */
  public AssertionsOnConsumedDelegate<Key, Value> havingPayloads(
      Consumer<List<Value>> valuesConsumer) {
    results.havingPayloads(valuesConsumer);
    return this;
  }

  /**
   * Evaluates the list of records to satisfy the given properties. Any kind of testing framework
   * can be used inside {@code recordsConsumer}.
   *
   * @param recordsConsumer Code testing the desired properties
   * @return This instance
   *
   * @see AssertionsOnConsumed#havingConsumerRecords(Consumer)
   */
  public AssertionsOnConsumedDelegate<Key, Value> havingConsumerRecords(
      Consumer<List<ConsumerRecord<Key, Value>>> recordsConsumer) {
    results.havingConsumerRecords(recordsConsumer);
    return this;
  }

  /**
   * Verifies if the list of values matches the given conditions.
   * @param matcher Condition to satisfy
   * @return This instance
   *
   * @see AssertionsOnConsumed#assertingThatPayloads(Matcher)
   */
  public AssertionsOnConsumedDelegate<Key, Value> assertingThatPayloads(
      Matcher<? super List<Value>> matcher) {
    results.assertingThatPayloads(matcher);
    return this;
  }
}
