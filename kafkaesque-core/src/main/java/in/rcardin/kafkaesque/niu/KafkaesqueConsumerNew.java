package in.rcardin.kafkaesque.niu;

import in.rcard.kafkaesque.KafkaesqueConsumerPollException;
import in.rcardin.kafkaesque.niu.KafkaesqueConsumerNew.KafkaesqueConsumerDelegate.DelegateCreationInfo;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
public class KafkaesqueConsumerNew<Key, Value> {

  private final KafkaConsumer<Key, Value> kafkaConsumer;
  
  private final long interval;
  private final TimeUnit timeUnit;
  private final int emptyPollsCount;
  private final long emptyPollsInterval;
  private final TimeUnit emptyPollsTimeUnit;
  
  private final DelegateCreationInfo<Key, Value> creationInfo;
  
  KafkaesqueConsumerNew(
      String brokersUrl,
      long interval,
      TimeUnit timeUnit,
      int emptyPollsCount,
      long emptyPollsInterval,
      TimeUnit emptyPollsTimeUnit,
      DelegateCreationInfo<Key, Value> creationInfo) {
    this.interval = interval;
    this.timeUnit = timeUnit;
    this.emptyPollsCount = emptyPollsCount;
    this.emptyPollsInterval = emptyPollsInterval;
    this.emptyPollsTimeUnit = emptyPollsTimeUnit;
    this.creationInfo = creationInfo;
    this.kafkaConsumer = createKafkaConsumer(brokersUrl);
  }
  
  private KafkaConsumer<Key, Value> createKafkaConsumer(String brokersUrl) {
    final Properties props = new Properties();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaesque-consumer");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersUrl);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        creationInfo.getKeyDeserializer().getClass());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        creationInfo.getValueDeserializer().getClass());
    final KafkaConsumer<Key, Value> consumer = new KafkaConsumer<>(props);
    subscribeConsumerToTopic(consumer, creationInfo.getTopic());
    return consumer;
  }
  
  private void subscribeConsumerToTopic(KafkaConsumer<Key, Value> consumer, String topic) {
    CountDownLatch latch = new CountDownLatch(1);
    consumer.subscribe(
        List.of(topic),
        new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
          
          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            latch.countDown();
//                    System.out.println("Assigned");
          }
        });
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .until(
            () -> {
              // The actual assignment of a topic to a consumer is done after a while
              // the consumer starts to poll messages. So, we forced the consumer to poll
              // from the topic and we wait until the consumer is assigned to the topic.
              consumer.poll(Duration.ofMillis(100));
              final boolean assigned = latch.getCount() == 0;
              if (assigned) {
                consumer.seekToBeginning(consumer.assignment());
              }
              return assigned;
            });
  }
  
  /**
   * Polls the broker and reads the messages contained in the configured topic.
   *
   * @return The read messages
   */
  ConsumedResultsAndKafkaesqueConsumerDelegatenew<Key, Value> poll() {
    try {
      final AtomicInteger emptyCycles = new AtomicInteger(emptyPollsCount);
      final List<ConsumerRecord<Key, Value>> readMessages = new ArrayList<>();
//      System.out.println("Empty cycles to await: " + emptyPollsCount);
      Awaitility.await()
          .atMost(interval, timeUnit)
          .pollInterval(emptyPollsInterval, emptyPollsTimeUnit)
          .until(
              () -> {
                if (readNewMessages(readMessages) == 0) {
                  final int remainingCycles = emptyCycles.decrementAndGet();
//                  System.out.println("Remaining empty cycles: " + remainingCycles);
//                  System.out.println(System.currentTimeMillis());
                  return remainingCycles == 0;
                }
                return false;
              });
      return new ConsumedResultsAndKafkaesqueConsumerDelegatenew<>(
          new ConsumedResults<>(readMessages),
          this
      );
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
    final ConsumerRecords<Key, Value> polled = kafkaConsumer.poll(Duration.ofMillis(50L));
    final List<ConsumerRecord<Key, Value>> newMessages = new ArrayList<>();
    polled.records(creationInfo.getTopic()).forEach(newMessages::add);
    if (!newMessages.isEmpty()) {
      readMessages.addAll(newMessages);
      return newMessages.size();
    }
    return 0;
  }

  /**
   * Closes the consumer. After the closing operation, the consumer cannot read any more messages.
   */
  public void andCloseConsumer() {
    kafkaConsumer.close();
  }

  /**
   * Creates instances of {@link KafkaesqueConsumerNew}.<br/>
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

    private final String brokerUrl;
    private String topic;
    private Deserializer<Key> keyDeserializer;
    private Deserializer<Value> valueDeserializer;
    private long interval = 60;
    private TimeUnit timeUnit = TimeUnit.SECONDS;
    private int emptyPollsCount = 2;
    private long emptyPollsInterval = 50L;
    private TimeUnit emptyPollsTimeUnit = TimeUnit.MILLISECONDS;
  
    private Builder(String brokerUrl) {
      this.brokerUrl = brokerUrl;
    }
  
    static <Key, Value> Builder<Key, Value> newInstance(String brokerUrl) {
      validateBrokerUrl(brokerUrl);
      return new Builder<>(brokerUrl);
    }
  
    private static void validateBrokerUrl(String brokerUrl) {
      if (brokerUrl == null) {
        throw new IllegalArgumentException("The broker url cannot be null");
      }
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
     * Creates an instance of the {@link KafkaesqueConsumerNew} and polls for messages. Before the
     * creation, it performs a set of validation steps.
     *
     * @return An instance of polled messages
     * @see ConsumedResultsAndKafkaesqueConsumerDelegatenew
     * @see ConsumedResults
     */
    public ConsumedResultsAndKafkaesqueConsumerDelegatenew<Key, Value> expecting() {
      validateInputs();
      final DelegateCreationInfo<Key, Value> creationInfo =
          new DelegateCreationInfo<>(topic, keyDeserializer, valueDeserializer);
      final KafkaesqueConsumerNew<Key, Value> consumer = new KafkaesqueConsumerNew<>(
          brokerUrl,
          interval,
          timeUnit,
          emptyPollsCount,
          emptyPollsInterval,
          emptyPollsTimeUnit,
          creationInfo);
      return consumer.poll();
    }

    private void validateInputs() {
      validateTopic();
      validateDeserializers();
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
