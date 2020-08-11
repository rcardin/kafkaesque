package in.rcard.kafkaesque;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

/**
 * Allows to test properties on the messages consumed by a {@link KafkaesqueConsumer}.
 * @param <Key> The type of the messages' keys
 * @param <Value> The type of the messages' values
 */
public class ConsumedResults<Key, Value> {

  private final Collection<ConsumerRecord<Key, Value>> consumerRecords;

  ConsumedResults(Collection<ConsumerRecord<Key, Value>> consumerRecords) {
    this.consumerRecords = consumerRecords;
  }

  /**
   * Tests if the number of consumed messages is equal to the given {@code size}.
   *
   * @param size The expected number of messages
   */
  public ConsumedResults<Key, Value> recordsSize(long size) {
    if (consumerRecords.size() != size) {
      throw new AssertionError(
          String.format(
              "The desired size of consumed messages %d is not equal to the effective"
                  + " number of read messages %d",
              size, consumerRecords.size()));
    }
    return this;
  }

  /**
   * Evaluates the list of headers to satisfy the given properties. Any kind of testing framework
   * can be used inside {@code headersConsumer}.
   *
   * @param headersConsumer Code testing the desired properties
   */
  public ConsumedResults<Key, Value> havingHeaders(Consumer<List<Headers>> headersConsumer) {
    var headersList =
        consumerRecords.stream().map(ConsumerRecord::headers).collect(Collectors.toList());
    headersConsumer.accept(headersList);
    return this;
  }

  /**
   * Evaluates the list of keys to satisfy the given properties. Any kind of testing framework can
   * be used inside {@code keysConsumer}.
   *
   * @param keysConsumer Code testing the desired properties
   */
  public ConsumedResults<Key, Value> havingKeys(Consumer<List<Key>> keysConsumer) {
    var keysList = consumerRecords.stream().map(ConsumerRecord::key).collect(Collectors.toList());
    keysConsumer.accept(keysList);
    return this;
  }

  /**
   * Evaluates the list of values to satisfy the given properties. Any kind of testing framework can
   * be used inside {@code valuesConsumer}.
   *
   * @param valuesConsumer Code testing the desired properties
   */
  public ConsumedResults<Key, Value> havingPayloads(Consumer<List<Value>> valuesConsumer) {
    var valuesList =
        consumerRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
    valuesConsumer.accept(valuesList);
    return this;
  }
}
