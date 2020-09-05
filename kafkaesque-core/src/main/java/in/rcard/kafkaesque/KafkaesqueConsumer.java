package in.rcard.kafkaesque;

import in.rcard.kafkaesque.KafkaesqueConsumer.KafkaesqueConsumerDelegate.DelegateCreationInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

/**
 * Represents a consumer that can read messages with key of type {@code Key}, and value of type
 * {@code Value}.
 *
 * @param <Key> Type of the key of a message
 * @param <Value> Type of the value of a message
 * @see Builder
 */
public class KafkaesqueConsumer<Key, Value> {

  private final long interval;
  private final TimeUnit timeUnit;
  private final int emptyPollsCount;
  private final long emptyPollsInterval;
  private final TimeUnit emptyPollsTimeUnit;

  private final KafkaesqueConsumerDelegate<Key, Value> consumerDelegate;

  KafkaesqueConsumer(
      long interval,
      TimeUnit timeUnit,
      int emptyPollsCount,
      long emptyPollsInterval,
      TimeUnit emptyPollsTimeUnit,
      KafkaesqueConsumerDelegate<Key, Value> consumerDelegate) {
    this.interval = interval;
    this.timeUnit = timeUnit;
    this.emptyPollsCount = emptyPollsCount;
    this.emptyPollsInterval = emptyPollsInterval;
    this.emptyPollsTimeUnit = emptyPollsTimeUnit;
    this.consumerDelegate = consumerDelegate;
  }

  /**
   * Polls the broker and reads the messages contained in the configured topic.
   *
   * @return The read messages
   */
  public ConsumedResults<Key, Value> poll() {
    try {
      final AtomicInteger emptyCycles = new AtomicInteger(emptyPollsCount);
      final List<ConsumerRecord<Key, Value>> readMessages = new ArrayList<>();
      Awaitility.await()
          .atMost(interval, timeUnit)
          .pollInterval(emptyPollsInterval, emptyPollsTimeUnit)
          .until(
              () -> {
                if (readNewMessages(readMessages) == 0) {
                  return emptyCycles.decrementAndGet() == 0;
                }
                return false;
              });
      return new ConsumedResults<>(readMessages);
    } catch (ConditionTimeoutException ctex) {
      throw new AssertionError(
          String.format(
              "The consumer reads new messages until the end of the given time interval: %d %s",
              interval, timeUnit.toString()));
    } catch (Exception ex) {
      throw new KafkaesqueConsumerPollException("Error during the poll operation", ex);
    }
  }

  private int readNewMessages(List<ConsumerRecord<Key, Value>> readMessages) {
    final List<ConsumerRecord<Key, Value>> newMessages = consumerDelegate.poll();
    if (newMessages != null && !newMessages.isEmpty()) {
      readMessages.addAll(newMessages);
      return newMessages.size();
    }
    return 0;
  }

  /**
   * Closes the consumer. After the closing operation, the consumer cannot read any more messages.
   */
  public void andCloseConsumer() {
    consumerDelegate.close();
  }

  /**
   * Creates instances of {@link KafkaesqueConsumer}.<br/>
   * There are defaults for some properties. In details, we have the following:
   *
   * <ol>
   *   <li>{@code waitingAtMost(200L, TimeUnit.MILLISECONDS)}</li>
   *   <li>{@code waitingEmptyPolls(2, 50L, TimeUnit.MILLISECONDS)}</li>
   * </ol>
   *
   * @param <Key> The type of the key of a message that the consumer can read
   * @param <Value> The type of the value of a message that the consumer can read
   */
  public static class Builder<Key, Value> {

    private String topic;
    private Deserializer<Key> keyDeserializer;
    private Deserializer<Value> valueDeserializer;
    private long interval = 200;
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    private int emptyPollsCount = 2;
    private long emptyPollsInterval = 50L;
    private TimeUnit emptyPollsTimeUnit = TimeUnit.MILLISECONDS;
  
  
    private final Function<
            DelegateCreationInfo<Key, Value>, ? extends KafkaesqueConsumerDelegate<Key, Value>>
        consumerDelegateFunction;
  
    private Builder(
        Function<DelegateCreationInfo<Key, Value>, ? extends KafkaesqueConsumerDelegate<Key, Value>>
            consumerDelegateFunction) {
      this.consumerDelegateFunction = consumerDelegateFunction;
    }

    static <Key, Value> Builder<Key, Value> newInstance(
        Function<DelegateCreationInfo<Key, Value>, ? extends KafkaesqueConsumerDelegate<Key, Value>>
            consumerDelegateFunction) {
      return new Builder<>(consumerDelegateFunction);
    }

    /**
     * Sets the topic to read from. This information is mandatory.
     *
     * @param topic The topic name
     */
    public Builder<Key, Value> fromTopic(String topic) {
      this.topic = topic;
      return this;
    }

    /**
     * Sets the key and value deserializers. This information is mandatory.
     *
     * @param keyDeserializer The key deserializer
     * @param valueDeserializer The value deserializer
     */
    public Builder<Key, Value> withDeserializers(
        Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer) {
      this.keyDeserializer = keyDeserializer;
      this.valueDeserializer = valueDeserializer;
      return this;
    }

    /**
     * Sets the time interval to wait until the receipt of all the produced messages. This
     * information is optional. The default values are {@code 200L} and
     * {@code TimeUnit.MILLISECOND}.
     *
     * @param interval Time interval
     * @param unit Unit of the time interval
     */
    public Builder<Key, Value> waitingAtMost(long interval, TimeUnit unit) {
      this.interval = interval;
      this.timeUnit = unit;
      return this;
    }

    /**
     * Sets the number of times a poll should return an empty list of messages to consider the read
     * phase concluded. This information is optional. The default values are {@code 2}, {@code 50L},
     * and {@code TimeUnit.MILLISECONDS}.
     *
     * @param count Number of empty polls
     * @param waitingInterval The interval to wait between two poll operations
     * @param waitingTimeUnit The time unit of the above interval
     */
    public Builder<Key, Value> waitingEmptyPolls(
        int count, long waitingInterval, TimeUnit waitingTimeUnit) {
      this.emptyPollsCount = count;
      this.emptyPollsInterval = waitingInterval;
      this.emptyPollsTimeUnit = waitingTimeUnit;
      return this;
    }

    /**
     * Creates a concrete instance of the {@link KafkaesqueConsumer}. Before the creation, performs
     * a set of validation steps.
     *
     * @return An instance of the {@link KafkaesqueConsumer}
     */
    public KafkaesqueConsumer<Key, Value> expecting() {
      validateInputs();
      final DelegateCreationInfo<Key, Value> creationInfo =
          new DelegateCreationInfo<>(topic, keyDeserializer, valueDeserializer);
      return new KafkaesqueConsumer<>(
          interval, timeUnit, emptyPollsCount, emptyPollsInterval, emptyPollsTimeUnit,
          consumerDelegateFunction.apply(creationInfo));
    }

    private void validateInputs() {
      validateConsumerDelegateFunction();
      validateTopic();
      validateDeserializers();
    }

    private void validateConsumerDelegateFunction() {
      if (consumerDelegateFunction == null) {
        throw new IllegalArgumentException(
            "The function creating the consumer delegate cannot be null");
      }
    }

    private void validateTopic() {
      if (topic == null || topic.isBlank()) {
        throw new IllegalArgumentException("The topic name cannot be empty");
      }
    }

    private void validateDeserializers() {
      if (keyDeserializer == null || valueDeserializer == null) {
        throw new IllegalArgumentException("The deserializers cannot be null");
      }
    }
  }

  /**
   * Represents the concrete Kafka consumer that uses a specific technology or library as
   * implementation (e.g. <a href="https://spring.io/projects/spring-kafka" target="_blank">Spring
   * Kafka</a>).
   *
   * @param <Key> The type of the messages' key
   * @param <Value> The type of the messages' value
   */
  interface KafkaesqueConsumerDelegate<Key, Value> {

    /**
     * Returns the messages that are available in a specific topic of a Kafka broker.
     *
     * @return A list of Kafka messages
     */
    List<ConsumerRecord<Key, Value>> poll();

    /**
     * Closes the consumer. Every call to the {@link #poll()} method after having close a consumer
     * <strong>must</strong> raise some king of exceptions.
     */
    void close();

    /**
     * Information needed to create a concrete Kafka consumer:
     *
     * <ul>
     *   <li>A topic
     *   <li>A key deserializer
     *   <li>A value deserializer
     * </ul>
     *
     * @param <Key> The type of the messages' key
     * @param <Value> The type of the messages' value
     */
    class DelegateCreationInfo<Key, Value> {
      private final String topic;
      private final Deserializer<Key> keyDeserializer;
      private final Deserializer<Value> valueDeserializer;

      private DelegateCreationInfo(
          String topic, Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer) {
        this.topic = topic;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
      }

      public String getTopic() {
        return topic;
      }

      public Deserializer<Key> getKeyDeserializer() {
        return keyDeserializer;
      }

      public Deserializer<Value> getValueDeserializer() {
        return valueDeserializer;
      }
    }
  }
}
