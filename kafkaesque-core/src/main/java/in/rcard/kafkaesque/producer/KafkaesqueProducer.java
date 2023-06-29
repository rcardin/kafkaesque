package in.rcard.kafkaesque.producer;

import in.rcard.kafkaesque.config.KafkaesqueConfigLoader;
import in.rcard.kafkaesque.config.KafkaesqueProducerConfig;
import in.rcard.kafkaesque.config.TypesafeKafkaesqueConfigLoader;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Represents a producer that sends messages with keys of type {@code Key} and with values of type
 * {@code Value}.
 *
 * @param <Key> Type of the key of a message
 * @param <Value> Type of the value of a message
 * @see KafkaesqueProducerDSL
 */
public final class KafkaesqueProducer<Key, Value> {

  private final KafkaProducer<Key, Value> kafkaProducer;
  private final DelegateCreationInfo<Key, Value> creationInfo;

  private final Duration forEachAckDuration;

  KafkaesqueProducer(
      String brokerUrl,
      Duration forEachAckDuration,
      DelegateCreationInfo<Key, Value> creationInfo) {
    this.forEachAckDuration = forEachAckDuration;
    this.creationInfo = creationInfo;
    this.kafkaProducer = createKafkaProducer(brokerUrl);
  }

  private KafkaProducer<Key, Value> createKafkaProducer(String brokerUrl) {
    Properties creationProps = new Properties();
    creationProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
    creationProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, creationInfo.getKeySerializer().getClass().getName());
    creationProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, creationInfo.getValueSerializer().getClass().getName());
    creationProps.put(ProducerConfig.ACKS_CONFIG, "all");

    final KafkaesqueConfigLoader kafkaesqueConfigLoader = new TypesafeKafkaesqueConfigLoader();

    final KafkaesqueProducerConfig producerConfig = kafkaesqueConfigLoader.loadProducerConfig();
    final Properties producerProperties = producerConfig.toProperties();
    producerProperties.putAll(creationProps);
    return new KafkaProducer<>(producerProperties);
  }

  ProducerRecord<Key, Value> sendRecord(Record<Key, Value> record) {
    try {
      final ProducerRecord<Key, Value> producerRecord = record.toPr(creationInfo.getTopic());
      CompletableFuture<RecordMetadata> promiseOnMetadata = sendSingleRecord(producerRecord);
      promiseOnMetadata.get(forEachAckDuration.toMillis(), TimeUnit.MILLISECONDS);
      return producerRecord;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AssertionError(
          String.format(
              "Impossible to send a the record %s in %d milliseconds",
              record, forEachAckDuration.toMillis()),
          e);
    }
  }

  List<ProducerRecord<Key, Value>> sendRecords(List<Record<Key, Value>> records) {
    final List<ProducerRecord<Key, Value>> producerRecords =
        records.stream().map(r -> r.toPr(creationInfo.getTopic())).collect(Collectors.toList());
    final List<CompletableFuture<RecordMetadata>> futures =
        producerRecords.stream().map(this::sendSingleRecord).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
          .get(forEachAckDuration.toMillis(), TimeUnit.MILLISECONDS);
      return producerRecords;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AssertionError(
          String.format(
              "At least the sending of one record of the list %s takes more than %d milliseconds",
              records, forEachAckDuration.toMillis()),
          e);
    }
  }

  private CompletableFuture<RecordMetadata> sendSingleRecord(ProducerRecord<Key, Value> record) {
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
   *
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
      return Objects.equals(key, record.key) && Objects.equals(value, record.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }

    @Override
    public String toString() {
      return "Record{" + "key=" + key + ", value=" + value + '}';
    }
  }
  
  static class DelegateCreationInfo<Key, Value> {
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
