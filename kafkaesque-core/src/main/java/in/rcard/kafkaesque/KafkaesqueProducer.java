package in.rcard.kafkaesque;

import in.rcard.kafkaesque.KafkaesqueProducer.KafkaesqueProducerDelegate.DelegateCreationInfo;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

/**
 * Represents a producer that sends messages with keys of type {@code Key} and with values of type
 * {@code Value}.
 *
 * @param <Key> Type of the key of a message
 * @param <Value> Type of the value of a message
 * @see Builder
 */
public final class KafkaesqueProducer<Key, Value> {

  private final KafkaProducer<Key, Value> kafkaProducer;
  private final DelegateCreationInfo<Key, Value> creationInfo;
  
  private final Duration forEachAckDuration;

  private final Duration waitForConsumerDuration;

  private final List<ProducerRecord<Key, Value>> records;

  KafkaesqueProducer(
      String brokerUrl,
      List<ProducerRecord<Key, Value>> records,
      Duration forEachAckDuration,
      Duration waitForConsumerDuration,
      DelegateCreationInfo<Key, Value> creationInfo) {
    this.records = records;
    this.forEachAckDuration = forEachAckDuration;
    this.waitForConsumerDuration = waitForConsumerDuration;
    this.creationInfo = creationInfo;
    this.kafkaProducer = createKafkaProducer(brokerUrl);
  }
  
  private KafkaProducer<Key,Value> createKafkaProducer(String brokerUrl) {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        creationInfo.getKeySerializer().getClass());
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        creationInfo.getValueSerializer().getClass());
    return new KafkaProducer<>(props);
  }
  
  /**
   * Asserts that some conditions hold on a single sent message.<br/>
   * For example:
   * <pre>
   *   producer.assertingAfterEach(
   *       pr -> assertThat(pr.key()).isEqualTo("key")
   *   );
   * </pre>
   *
   * @param messageConsumer The conditions that must hold on a message
   */
  public void assertingAfterEach(Consumer<ProducerRecord<Key, Value>> messageConsumer) {
    records.forEach(
        record -> {
          sendRecord(record);
          consume(messageConsumer, record);
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

  private void sendRecord(ProducerRecord<Key, Value> record) {
    try {
      CompletableFuture<RecordMetadata> promiseOnMetadata = sendSingleRecord(record);
      promiseOnMetadata.get(forEachAckDuration.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AssertionError(
          String.format(
              "Impossible to send a the record %s in %d milliseconds",
              record, forEachAckDuration.toMillis()),
          e);
    }
  }
  
  /**
   * Asserts that some conditions hold on the whole list of sent message.<br>
   * For example:
   *
   * <pre>
   *   producer.assertingAfterAll(
   *       records -> assertThat(records).hasSize(2)
   *   );
   * </pre>
   *
   * @param messagesConsumer The conditions that must hold on the whole list of messages
   */
  public void assertingAfterAll(Consumer<List<ProducerRecord<Key, Value>>> messagesConsumer) {
    sendRecords();
    consume(messagesConsumer);
  }
  
  private void consume(Consumer<List<ProducerRecord<Key, Value>>> messagesConsumer) {
    try {
      Awaitility.await()
          .atMost(waitForConsumerDuration)
          .untilAsserted(() -> messagesConsumer.accept(records));
    } catch (ConditionTimeoutException ex) {
      throw new AssertionError(
          String.format(
              "The consuming of the list of messages %s takes more than %d milliseconds",
              records, waitForConsumerDuration.toMillis()));
    }
  }
  
  private void sendRecords() {
    final List<CompletableFuture<RecordMetadata>> futures =
        records.stream().map(this::sendSingleRecord).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
          .get(forEachAckDuration.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AssertionError(
          String.format(
              "At least the sending of one record of the list %s takes more than %d milliseconds",
              records, forEachAckDuration.toMillis()),
          e);
    }
  }
  
  private CompletableFuture<RecordMetadata> sendSingleRecord(
      ProducerRecord<Key, Value> record) {
    CompletableFuture<RecordMetadata> promiseOnMetadata = new CompletableFuture<>();
    kafkaProducer.send(
        record,
        (metadata, exception) -> {
          if (exception == null) {
            promiseOnMetadata.complete(metadata);
          } else {
            promiseOnMetadata.completeExceptionally(exception);
          }
        });
    return promiseOnMetadata;
  }
  
  /**
   * A record to a producer can send to a Kafka topic.
   * @param <Key> The type of the key
   * @param <Value> The type of the value
   */
  public static class Record<Key, Value> {
    private final Key key;
    private final Value value;
    
    private Record(Key key, Value value) {
      this.key = key;
      this.value = value;
    }
    
    public static <Key, Value> Record<Key, Value> of(Key key, Value value) {
      return new Record<>(key, value);
    }
    
    public static <Key, Value> Record<Key, Value> of(ProducerRecord<Key, Value> producerRecord) {
      return new Record<>(producerRecord.key(), producerRecord.value());
    }
    
    public ProducerRecord<Key, Value> toPr(String topic) {
      return new ProducerRecord<>(topic, key, value);
    }
    
    public Key getKey() {
      return key;
    }
    
    public Value getValue() {
      return value;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Record<?, ?> record = (Record<?, ?>) o;
      return Objects.equals(key, record.key) &&
                 Objects.equals(value, record.value);
    }
    
    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }
    
    @Override
    public String toString() {
      return "Record{" +
                 "key=" + key +
                 ", value=" + value +
                 '}';
    }
  }

  /**
   * Creates instances of {@link KafkaesqueProducer}.<br/>
   * There are defaults for some properties. In details, we have the following:
   *
   * <ol>
   *   <li>{@code waitingAtMostForEachAck(200L, TimeUnit.MILLISECONDS)}
   *   <li>{@code waitingForTheConsumerAtMost(500L, TimeUnit.MILLISECONDS)}
   * </ol>
   *
   * @param <Key> The type of the key of a message that the consumer can read
   * @param <Value> The type of the value of a message that the consumer can read
   */
  public static class Builder<Key, Value> {
  
    private final String brokerUrl;
    private String topic;
    private Serializer<Key> keySerializer;
    private Serializer<Value> valueSerializer;
    private List<Record<Key, Value>> records;
    private long waitingAtMostForEachAckInterval = 200L;
    private TimeUnit waitingAtMostForEachAckTimeUnit = TimeUnit.MILLISECONDS;
    private long waitingForTheConsumerAtMostInterval = 500L;
    private TimeUnit waitingForTheConsumerAtMostTimeUnit = TimeUnit.MILLISECONDS;

    private Builder(String brokerUrl) {
      validateBrokerUrl(brokerUrl);
      this.brokerUrl = brokerUrl;
    }
  
    private void validateBrokerUrl(String brokerUrl) {
      if (brokerUrl == null || brokerUrl.isEmpty()) {
        throw new IllegalArgumentException("The brokerUrl cannot be empty");
      }
    }
  
    static <Key, Value> Builder<Key, Value> newInstance(String brokerUrl) {
      return new Builder<>(brokerUrl);
    }
  
    /**
     * Sets the topic to write to. This information is mandatory.
     * @param topic The name of the topic
     */
    public Builder<Key, Value> toTopic(String topic) {
      this.topic = topic;
      return this;
    }
  
    /**
     * Sets the serializers for the keys and values of the messages. This information is mandatory.
     * @param keySerializer The key serializer
     * @param valueSerializer The value serializer
     */
    public Builder<Key, Value> withSerializers(
        Serializer<Key> keySerializer, Serializer<Value> valueSerializer) {
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
      return this;
    }
  
    /**
     * Sets the list of messages to write to the target topic. This information is mandatory.
     * @param records The list of messages
     */
    public Builder<Key, Value> messages(List<Record<Key, Value>> records) {
      this.records = records;
      return this;
    }

    /**
     * Sets the time interval to wait for each ack from the broker. This information is optional.
     * The default values are {@code 200L} and * {@code TimeUnit.MILLISECOND}.
     *
     * @param interval Time interval
     * @param unit Unit of the time interval
     */
    public Builder<Key, Value> waitingAtMostForEachAck(long interval, TimeUnit unit) {
      this.waitingAtMostForEachAckInterval = interval;
      this.waitingAtMostForEachAckTimeUnit = unit;
      return this;
    }
  
    /**
     * Sets the time interval to wait for the consumer to run on the sent messages. This information
     * is optional. The default values are {@code 500L} and * {@code TimeUnit.MILLISECOND}.
     *
     * @param interval Time interval
     * @param unit Unit of the time interval
     */
    public Builder<Key, Value> waitingForTheConsumerAtMost(long interval, TimeUnit unit) {
      this.waitingForTheConsumerAtMostInterval = interval;
      this.waitingForTheConsumerAtMostTimeUnit = unit;
      return this;
    }

    /**
     * Creates an instance of the {@link KafkaesqueProducer}. Before the creation, performs a set of
     * validation steps.
     *
     * @return An instance of the {@link KafkaesqueProducer}
     */
    public KafkaesqueProducer<Key, Value> expecting() {
      validateInputs();
      final DelegateCreationInfo<Key, Value> creationInfo = new DelegateCreationInfo<>(
          topic, keySerializer, valueSerializer);
      return new KafkaesqueProducer<>(
          brokerUrl,
          createProducerRecords(),
          Duration.of(
              waitingAtMostForEachAckInterval, waitingAtMostForEachAckTimeUnit.toChronoUnit()),
          Duration.of(
              waitingForTheConsumerAtMostInterval,
              waitingForTheConsumerAtMostTimeUnit.toChronoUnit()),
          creationInfo);
    }
  
    private List<ProducerRecord<Key, Value>> createProducerRecords() {
      return records
                 .stream()
                 .map(record -> record.toPr(topic))
                 .collect(Collectors.toList());
    }
  
    private void validateInputs() {
      validateTopic();
      validateRecords();
      validateSerializers();
    }
    
    private void validateTopic() {
      if (topic == null || topic.isBlank()) {
        throw new IllegalArgumentException("The topic name cannot be empty");
      }
    }

    private void validateRecords() {
      if (records == null || records.isEmpty()) {
        throw new IllegalArgumentException("The list of records to send cannot be empty");
      }
    }

    private void validateSerializers() {
      if (keySerializer == null || valueSerializer == null) {
        throw new IllegalArgumentException("The serializers cannot be null");
      }
    }
  }

  /**
   * Represents the concrete Kafka producer that uses a specific technology or library as
   * implementation (e.g. <a href="https://spring.io/projects/spring-kafka" target="_blank">Spring
   * Kafka</a>). It sends the records to the embedded Kafka broker.
   *
   * @param <Key> The type of the messages' key
   * @param <Value> The type of the messages' value
   */
  interface KafkaesqueProducerDelegate<Key, Value> {
  
    /**
     * Sends the {@code record} to the embedded Kafka broker.
     * @param record The message to send
     * @return A future on the result of the sending operation
     */
    CompletableFuture<RecordMetadata> sendRecord(ProducerRecord<Key, Value> record);

    class DelegateCreationInfo<Key, Value> {
      private final String topic;
      private final Serializer<Key> keySerializer;
      private final Serializer<Value> valueSerializer;

      public DelegateCreationInfo(
          String topic, Serializer<Key> keySerializer, Serializer<Value> valueSerializer) {
        this.topic = topic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
      }
  
      public String getTopic() {
        return topic;
      }

      public Serializer<Key> getKeySerializer() {
        return keySerializer;
      }

      public Serializer<Value> getValueSerializer() {
        return valueSerializer;
      }
    }
  }
}
