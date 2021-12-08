package in.rcardin.kafkaesque.niu;

import java.util.List;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.hamcrest.Matcher;

/**
 * A delegate type that allows the access to method both of the type {@link in.rcard.kafkaesque.KafkaesqueConsumer}, and
 * the type {@link ConsumedResults}.
 *
 * @param <Key> The type of the messages' keys
 * @param <Value> The type of the messages' values
 *
 * @see ConsumedResults
 * @see in.rcard.kafkaesque.KafkaesqueConsumer
 */
class ConsumedResultsAndKafkaesqueConsumerDelegatenew<Key, Value> {
  private final ConsumedResults<Key, Value> results;
  private final KafkaesqueConsumerNew<Key, Value> consumer;

  ConsumedResultsAndKafkaesqueConsumerDelegatenew(
      ConsumedResults<Key, Value> results, KafkaesqueConsumerNew<Key, Value> consumer) {
    this.results = results;
    this.consumer = consumer;
  }
  
  /**
   * @see KafkaesqueConsumerNew#andCloseConsumer()
   */
  public void andCloseConsumer() {
    consumer.andCloseConsumer();
  }
  
  /**
   * @see ConsumedResults#havingRecordsSize(long)
   */
  public ConsumedResultsAndKafkaesqueConsumerDelegatenew<Key, Value> havingRecordsSize(long size) {
    results.havingRecordsSize(size);
    return this;
  }
  
  /**
   * @see ConsumedResults#havingHeaders(Consumer)
   */
  public ConsumedResultsAndKafkaesqueConsumerDelegatenew<Key, Value> havingHeaders(
      Consumer<List<Headers>> headersConsumer) {
    results.havingHeaders(headersConsumer);
    return this;
  }
  
  /**
   * @see ConsumedResults#havingKeys(Consumer)
   */
  public ConsumedResultsAndKafkaesqueConsumerDelegatenew<Key, Value> havingKeys(
      Consumer<List<Key>> keysConsumer) {
    results.havingKeys(keysConsumer);
    return this;
  }
  
  /**
   * @see ConsumedResults#havingPayloads(Consumer)
   */
  public ConsumedResultsAndKafkaesqueConsumerDelegatenew<Key, Value> havingPayloads(
      Consumer<List<Value>> valuesConsumer) {
    results.havingPayloads(valuesConsumer);
    return this;
  }
  
  /**
   * @see ConsumedResults#havingConsumerRecords(Consumer)
   */
  public ConsumedResultsAndKafkaesqueConsumerDelegatenew<Key, Value> havingConsumerRecords(
      Consumer<List<ConsumerRecord<Key, Value>>> recordsConsumer) {
    results.havingConsumerRecords(recordsConsumer);
    return this;
  }
  
  /**
   * @see ConsumedResults#assertingThatPayloads(Matcher)
   */
  public ConsumedResultsAndKafkaesqueConsumerDelegatenew<Key, Value> assertingThatPayloads(
      Matcher<? super List<Value>> matcher) {
    results.assertingThatPayloads(matcher);
    return this;
  }
}
