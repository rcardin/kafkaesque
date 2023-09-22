package in.rcard.kafkaesque.producer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
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
    creationProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        creationInfo.getKeySerializer().getClass().getName());
    creationProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        creationInfo.getValueSerializer().getClass().getName());
    creationProps.put(ProducerConfig.ACKS_CONFIG, "all");

    creationProps.putAll(creationInfo.getProducerProperties());
    return new KafkaProducer<>(creationProps);
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
   * A record a producer can send to a Kafka topic.
   *
   * @param <Key> The type of the key
   * @param <Value> The type of the value
   */
  public static class Record<Key, Value> {
    private final Key key;
    private final Value value;

    private final List<Header> headers;

    private Record(Key key, Value value, List<Header> headers) {
      this.key = key;
      this.value = value;
      this.headers = headers;
    }

    public static <Key, Value> Record<Key, Value> of(Key key, Value value, Header... headers) {
      return new Record<>(key, value, List.of(headers));
    }

    public static <Key, Value> Record<Key, Value> of(ProducerRecord<Key, Value> producerRecord) {
      return new Record<>(
          producerRecord.key(),
          producerRecord.value(),
          adaptKafkaHeader(producerRecord.headers().toArray()));
    }

    private static List<Header> adaptKafkaHeader(org.apache.kafka.common.header.Header[] array) {
      return Stream.of(array)
          .map(kafkaHeader -> Header.header(kafkaHeader.key(), kafkaHeader.value()))
          .collect(Collectors.toList());
    }

    public ProducerRecord<Key, Value> toPr(String topic) {
      final ProducerRecord<Key, Value> producerRecord = new ProducerRecord<>(topic, key, value);
      addHeaders(producerRecord);
      return producerRecord;
    }

    private void addHeaders(ProducerRecord<Key, Value> producerRecord) {
      final Headers kafkaHeaders = producerRecord.headers();
      headers.forEach(h -> kafkaHeaders.add(h.toKafkaHeader()));
    }

    public Key getKey() {
      return key;
    }

    public Value getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Record<?, ?> record = (Record<?, ?>) o;
      return Objects.equals(key, record.key) && Objects.equals(value, record.value) && Objects.equals(headers, record.headers);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value, headers);
    }

    @Override
    public String toString() {
      return "Record{" +
              "key=" + key +
              ", value=" + value +
              ", headers=" + headers +
              '}';
    }
  }

  /** A header to add to a message sent to a Kafka topic. */
  public static class Header {
    private final String key;
    private final byte[] value;

    private Header(String key, byte[] value) {
      this.key = key;
      this.value = value;
    }

    /**
     * Creates a new header with the given key and value.
     * @param key The key of the header
     * @param value The value of the header
     * @return The new header
     * @throws IllegalArgumentException If the key or the value are null or empty
     */
    public static Header header(String key, String value) {
      if (key == null || key.isEmpty()) {
        throw new IllegalArgumentException("The key of the header cannot be null or empty");
      }
      if (value == null || value.isEmpty()) {
        throw new IllegalArgumentException("The value of the header cannot be null or empty");
      }
      return new Header(key, value.getBytes());
    }

    /**
     * Creates a new header with the given key and value.
     * @param key The key of the header
     * @param value The value of the header
     * @return The new header
     * @throws IllegalArgumentException If the key or the value are null or empty
     */
    public static Header header(String key, byte[] value) {
      if (key == null || key.isEmpty()) {
        throw new IllegalArgumentException("The key of the header cannot be null or empty");
      }
      if (value == null || value.length == 0) {
        throw new IllegalArgumentException("The value of the header cannot be null or empty");
      }
      return new Header(key, value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Header header = (Header) o;
      return Objects.equals(key, header.key) && Arrays.equals(value, header.value);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(key);
      result = 31 * result + Arrays.hashCode(value);
      return result;
    }

    public org.apache.kafka.common.header.Header toKafkaHeader() {
      return new RecordHeader(key, value);
    }

    @Override
    public String toString() {
      return "Header{" +
              "key='" + key + '\'' +
              ", value=" + Arrays.toString(value) +
              '}';
    }
  }

  static class DelegateCreationInfo<Key, Value> {
    private final String topic;
    private final Serializer<Key> keySerializer;
    private final Serializer<Value> valueSerializer;

    private final Properties producerProperties;

    public DelegateCreationInfo(
        String topic,
        Serializer<Key> keySerializer,
        Serializer<Value> valueSerializer,
        Properties producerProperties) {
      this.topic = topic;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
      this.producerProperties = producerProperties;
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

    public Properties getProducerProperties() {
      return producerProperties;
    }
  }
}
