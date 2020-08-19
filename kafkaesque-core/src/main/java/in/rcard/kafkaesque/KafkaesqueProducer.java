package in.rcard.kafkaesque;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.awaitility.Awaitility;

public final class KafkaesqueProducer<Key, Value> {

  private final Duration forEachAckDuration = Duration.of(200L, ChronoUnit.MILLIS);

  private final Duration forAllAcksDuration = Duration.of(1L, ChronoUnit.SECONDS);

  private final Duration waitForConsumerDuration = Duration.of(500L, ChronoUnit.MILLIS);

  private final List<ProducerRecord<Key, Value>> records;

  private final KafkaesqueProducerDelegate<Key, Value> producerDelegate;

  KafkaesqueProducer(
      List<ProducerRecord<Key, Value>> records,
      KafkaesqueProducerDelegate<Key, Value> producerDelegate) {
    this.records = records;
    this.producerDelegate = producerDelegate;
  }

  KafkaesqueProducer<Key, Value> assertingAfterEach(
      Consumer<ProducerRecord<Key, Value>> messageConsumer) {
    records.forEach(
        record -> {
          try {
            producerDelegate
                .sendRecord(record)
                .get(forEachAckDuration.toMillis(), TimeUnit.MILLISECONDS);
          } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new AssertionError(e);
          }
          Awaitility.await()
              .atMost(waitForConsumerDuration)
              .untilAsserted(() -> messageConsumer.accept(record));
        });
    return this;
  }

  KafkaesqueProducer<Key, Value> assertingAfterAll(
      Consumer<List<ProducerRecord<Key, Value>>> messagesConsumer) {
    final List<CompletableFuture<RecordMetadata>> futures =
        records.stream().map(producerDelegate::sendRecord).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
          .get(forAllAcksDuration.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AssertionError(e);
    }
    Awaitility.await()
        .atMost(waitForConsumerDuration)
        .untilAsserted(() -> messagesConsumer.accept(records));
    return this;
  }

  static class Builder<Key, Value> {
  
    private String topic;
    private Serializer<Key> keySerializer;
    private Serializer<Value> valueSerializer;
    private List<ProducerRecord<Key, Value>> records;
    private long waitingAtMostForEachAckInterval;
    private TimeUnit waitingAtMostForEachAckTimeUnit;
    private long waitingAtMostForAllAcksInteval;
    private TimeUnit waitingAtMostForAllAcksTimeUnit;
    private long waitingForTheConsumerAtMostInterval;
    private TimeUnit waitingForTheConsumerAtMostTimeUnit;
  
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
      return null;
    }
  
    private void validateInputs() {
      validateTopic();
      validateRecords();
      validateSerializers();
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
  }
}
