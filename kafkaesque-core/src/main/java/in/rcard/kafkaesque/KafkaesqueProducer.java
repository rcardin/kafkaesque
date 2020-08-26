package in.rcard.kafkaesque;

import in.rcard.kafkaesque.KafkaesqueProducer.KafkaesqueProducerDelegate.DelegateCreationInfo;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.awaitility.Awaitility;

public final class KafkaesqueProducer<Key, Value> {

  private final Duration forEachAckDuration;

  private final Duration forAllAcksDuration;

  private final Duration waitForConsumerDuration;

  private final List<ProducerRecord<Key, Value>> records;

  private final KafkaesqueProducerDelegate<Key, Value> producerDelegate;

  KafkaesqueProducer(
      List<ProducerRecord<Key, Value>> records,
      KafkaesqueProducerDelegate<Key, Value> producerDelegate,
      Duration forEachAckDuration,
      Duration forAllAcksDuration,
      Duration waitForConsumerDuration) {
    this.records = records;
    this.producerDelegate = producerDelegate;
    this.forEachAckDuration = forEachAckDuration;
    this.forAllAcksDuration = forAllAcksDuration;
    this.waitForConsumerDuration = waitForConsumerDuration;
  }

  KafkaesqueProducer<Key, Value> assertingAfterEach(
      Consumer<ProducerRecord<Key, Value>> messageConsumer) {
    records.forEach(
        record -> {
          sendRecord(record);
          Awaitility.await()
              .atMost(waitForConsumerDuration)
              .untilAsserted(() -> messageConsumer.accept(record));
        });
    return this;
  }
  
  private void sendRecord(ProducerRecord<Key, Value> record) {
    try {
      producerDelegate
          .sendRecord(record)
          .get(forEachAckDuration.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AssertionError(e);
    }
  }
  
  KafkaesqueProducer<Key, Value> assertingAfterAll(
      Consumer<List<ProducerRecord<Key, Value>>> messagesConsumer) {
    sendRecords();
    Awaitility.await()
        .atMost(waitForConsumerDuration)
        .untilAsserted(() -> messagesConsumer.accept(records));
    return this;
  }
  
  private void sendRecords() {
    final List<CompletableFuture<RecordMetadata>> futures =
        records.stream().map(producerDelegate::sendRecord).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
          .get(forAllAcksDuration.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AssertionError(e);
    }
  }
  
  static class Builder<Key, Value> {

    private final Function<DelegateCreationInfo<Key, Value>, KafkaesqueProducerDelegate<Key, Value>>
        creationInfoFunction;
    private String topic;
    private Serializer<Key> keySerializer;
    private Serializer<Value> valueSerializer;
    private List<ProducerRecord<Key, Value>> records;
    private long waitingAtMostForEachAckInterval = 200L;
    private TimeUnit waitingAtMostForEachAckTimeUnit = TimeUnit.MILLISECONDS;
    private long waitingAtMostForAllAcksInteval = 1L;
    private TimeUnit waitingAtMostForAllAcksTimeUnit = TimeUnit.SECONDS;
    private long waitingForTheConsumerAtMostInterval = 500L;
    private TimeUnit waitingForTheConsumerAtMostTimeUnit = TimeUnit.MILLISECONDS;

    Builder(
        Function<
                    KafkaesqueProducerDelegate.DelegateCreationInfo<Key, Value>,
                    KafkaesqueProducerDelegate<Key, Value>>
            creationInfoFunction) {
      this.creationInfoFunction = creationInfoFunction;
    }

    static <Key, Value> Builder<Key, Value> newInstance(
        Function<
                KafkaesqueProducerDelegate.DelegateCreationInfo<Key, Value>,
                KafkaesqueProducerDelegate<Key, Value>>
            creationInfoFunction) {
      return new Builder<>(creationInfoFunction);
    }

    Builder<Key, Value> toTopic(String topic) {
      this.topic = topic;
      return this;
    }

    Builder<Key, Value> withSerializers(
        Serializer<Key> keySerializer, Serializer<Value> valueSerializer) {
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
      return this;
    }

    Builder<Key, Value> messages(List<ProducerRecord<Key, Value>> records) {
      this.records = records;
      return this;
    }

    Builder<Key, Value> waitingAtMostForEachAck(long interval, TimeUnit unit) {
      this.waitingAtMostForEachAckInterval = interval;
      this.waitingAtMostForEachAckTimeUnit = unit;
      return this;
    }

    Builder<Key, Value> waitingAtMostForAllAcks(long interval, TimeUnit unit) {
      this.waitingAtMostForAllAcksInteval = interval;
      this.waitingAtMostForAllAcksTimeUnit = unit;
      return this;
    }

    Builder<Key, Value> waitingForTheConsumerAtMost(long interval, TimeUnit unit) {
      this.waitingForTheConsumerAtMostInterval = interval;
      this.waitingForTheConsumerAtMostTimeUnit = unit;
      return this;
    }

    KafkaesqueProducer<Key, Value> expecting() {
      validateInputs();
      final KafkaesqueProducerDelegate<Key, Value> producerDelegate =
          creationInfoFunction.apply(
              new DelegateCreationInfo<>(topic, keySerializer, valueSerializer));
      return new KafkaesqueProducer<>(
          records,
          producerDelegate,
          Duration.of(
              waitingAtMostForEachAckInterval, waitingAtMostForEachAckTimeUnit.toChronoUnit()),
          Duration.of(
              waitingAtMostForAllAcksInteval, waitingAtMostForAllAcksTimeUnit.toChronoUnit()),
          Duration.of(
              waitingForTheConsumerAtMostInterval,
              waitingForTheConsumerAtMostTimeUnit.toChronoUnit()));
    }

    private void validateInputs() {
      validateProducerDelegateFunction();
      validateTopic();
      validateRecords();
      validateSerializers();
    }
  
    private void validateProducerDelegateFunction() {
      if (creationInfoFunction == null) {
        throw new IllegalArgumentException(
            "The function creating the producer delegate cannot be null");
      }
    }
  
    private void validateTopic() {
      if (topic == null || topic.isBlank()) {
        throw new AssertionError("The topic name cannot be empty");
      }
    }

    private void validateRecords() {
      if (records == null || records.isEmpty()) {
        throw new AssertionError("The list of records to send cannot be empty");
      }
    }

    private void validateSerializers() {
      if (keySerializer == null || valueSerializer == null) {
        throw new AssertionError("The serializers cannot be null");
      }
    }
  }

  /**
   * Sends the record to the embedded Kafka broker.
   *
   * @param <Key> The type of the message's key
   * @param <Value> The type of the message's value
   */
  interface KafkaesqueProducerDelegate<Key, Value> {
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
