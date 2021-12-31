package in.rcard.kafkaesque.producer;

import in.rcard.kafkaesque.producer.KafkaesqueProducer.DelegateCreationInfo;
import in.rcard.kafkaesque.producer.KafkaesqueProducer.Record;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Creates instances of {@link KafkaesqueProducer}.<br>
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
public class KafkaesqueProducerDSL<Key, Value> {

  private final String brokerUrl;
  private String topic;
  private Serializer<Key> keySerializer;
  private Serializer<Value> valueSerializer;
  private List<Record<Key, Value>> records;
  private long waitingAtMostForEachAckInterval = 200L;
  private TimeUnit waitingAtMostForEachAckTimeUnit = TimeUnit.MILLISECONDS;
  private long waitingForTheConsumerAtMostInterval = 500L;
  private TimeUnit waitingForTheConsumerAtMostTimeUnit = TimeUnit.MILLISECONDS;

  private KafkaesqueProducerDSL(String brokerUrl) {
    validateBrokerUrl(brokerUrl);
    this.brokerUrl = brokerUrl;
  }

  private void validateBrokerUrl(String brokerUrl) {
    if (brokerUrl == null || brokerUrl.isEmpty()) {
      throw new IllegalArgumentException("The brokerUrl cannot be empty");
    }
  }

  public static <Key, Value> KafkaesqueProducerDSL<Key, Value> newInstance(String brokerUrl) {
    return new KafkaesqueProducerDSL<>(brokerUrl);
  }

  /**
   * Sets the topic to write to. This information is mandatory.
   *
   * @param topic The name of the topic
   */
  public KafkaesqueProducerDSL<Key, Value> toTopic(String topic) {
    this.topic = topic;
    return this;
  }

  /**
   * Sets the serializers for the keys and values of the messages. This information is mandatory.
   *
   * @param keySerializer The key serializer
   * @param valueSerializer The value serializer
   */
  public KafkaesqueProducerDSL<Key, Value> withSerializers(
      Serializer<Key> keySerializer, Serializer<Value> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    return this;
  }

  /**
   * Sets the list of messages to write to the target topic. This information is mandatory.
   *
   * @param records The list of messages
   */
  public KafkaesqueProducerDSL<Key, Value> messages(List<Record<Key, Value>> records) {
    this.records = records;
    return this;
  }

  /**
   * Sets the time interval to wait for each ack from the broker. This information is optional. The
   * default values are {@code 200L} and * {@code TimeUnit.MILLISECOND}.
   *
   * @param interval Time interval
   * @param unit Unit of the time interval
   */
  public KafkaesqueProducerDSL<Key, Value> waitingAtMostForEachAck(long interval, TimeUnit unit) {
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
  public KafkaesqueProducerDSL<Key, Value> waitingForTheConsumerAtMost(
      long interval, TimeUnit unit) {
    this.waitingForTheConsumerAtMostInterval = interval;
    this.waitingForTheConsumerAtMostTimeUnit = unit;
    return this;
  }

  public AfterAllAssertions<Key, Value> andAfterAll() {
    final KafkaesqueProducer<Key, Value> producer = buildKafkaesqueProducer();
    return new AfterAllAssertions<Key, Value>(
        producer,
        records,
        Duration.of(
            waitingAtMostForEachAckInterval, waitingAtMostForEachAckTimeUnit.toChronoUnit()));
  }

  public AfterEachAssertions<Key, Value> andAfterEach() {
    final KafkaesqueProducer<Key, Value> producer = buildKafkaesqueProducer();
    return new AfterEachAssertions<Key, Value>(
        producer,
        records,
        Duration.of(
            waitingAtMostForEachAckInterval, waitingAtMostForEachAckTimeUnit.toChronoUnit()));
  }

  /**
   * Creates an instance of the {@link KafkaesqueProducer}. Before the creation, performs a set of
   * validation steps.
   *
   * @return An instance of the {@link KafkaesqueProducer}
   */
  private KafkaesqueProducer<Key, Value> buildKafkaesqueProducer() {
    validateInputs();
    final DelegateCreationInfo<Key, Value> creationInfo =
        new DelegateCreationInfo<>(topic, keySerializer, valueSerializer);
    return new KafkaesqueProducer<>(
        brokerUrl,
        Duration.of(
            waitingForTheConsumerAtMostInterval,
            waitingForTheConsumerAtMostTimeUnit.toChronoUnit()),
        creationInfo);
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
