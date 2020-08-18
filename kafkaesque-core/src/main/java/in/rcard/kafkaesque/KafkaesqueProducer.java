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
  private final TimeUnit waitForConsumerTimeUnit = TimeUnit.MILLISECONDS;

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

  interface Builder<K, Key, Value> {
    Builder<K, Key, Value> toTopic(String topic);

    Builder<K, Key, Value> withSerializers(
        Serializer<Key> keySerializer, Serializer<Value> valueSerializer);

    Builder<K, Key, Value> messages(List<ProducerRecord<Key, Value>> records);

    Builder<K, Key, Value> waitingAtMostForEachAck(long interval, TimeUnit unit);

    Builder<K, Key, Value> waitingAtMostForAllAcks(long interval, TimeUnit unit);

    Builder<K, Key, Value> waitingForTheConsumerAtMost(long interval, TimeUnit unit);

    KafkaesqueProducer<Key, Value> expecting();
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
