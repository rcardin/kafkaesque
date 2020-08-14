package in.rcard.kafkaesque;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Represents a consumer that can read messages with key of type {@code Key}, and value of type
 * {@code Value}.
 *
 * @param <Key> Type of the key of a message
 * @param <Value> Type of the value of a message
 * @see Builder
 */
public interface KafkaesqueConsumer<Key, Value> {

  /**
   * <p>Creates instances of {@link KafkaesqueConsumer}.
   * </p>
   * <p>A valid implementation <strong>must</strong> expose a {@code static} method called {@code
   * newInstance} that returns an instance of the builder and as a single parameter of type {@code
   * K}.<br>
   * For example:
   *
   * <pre>
   * public static <Key, Value> Builder<Key, Value> newInstance(
   *     EmbeddedKafkaBroker embeddedKafkaBroker)
   * </pre>
   *
   * @param <K> The type of the embedded Kafka broker
   * @param <Key> The type of the key of a message that the consumer can read
   * @param <Value> The type of the value of a message that the consumer can read
   */
  interface Builder<K, Key, Value> {
  
    /**
     * Sets the topic to read from. This information should be mandatory.
     * @param topic The topic name
     */
    Builder<K, Key, Value> fromTopic(String topic);
  
    /**
     * Sets the key and value deserializers. This information should be mandatory.
     * @param keyDeserializer The key deserializer
     * @param valueDeserializer The value deserializer
     */
    Builder<K, Key, Value> withDeserializers(
        Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer);
  
    /**
     * Sets the time interval to wait until the receipt of all the produced messages.
     * This information should be optional, providing a default.
     * @param interval Time interval
     * @param unit Unit of the time interval
     */
    Builder<K, Key, Value> waitingAtMost(long interval, TimeUnit unit);
  
    /**
     * Creates a concrete instance of the {@link KafkaesqueConsumer}. Before the creation, performs
     * a set of validation steps.
     * @return An instance of the {@link KafkaesqueConsumer}
     */
    KafkaesqueConsumer<Key, Value> expecting();
  }
  
  /**
   * Polls the broker and reads the messages contained in the configured topic.
   * @return The read messages
   */
  ConsumedResults<Key, Value> poll();
}
