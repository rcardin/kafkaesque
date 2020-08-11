package in.rcard.kafkaesque;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.awaitility.Awaitility;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.StringUtils;

public class SpringKafkaesqueConsumer<Key, Value> implements KafkaesqueConsumer<Key, Value> {

  private final KafkaMessageListenerContainer<Key, Value> container;
  private final BlockingQueue<ConsumerRecord<Key, Value>> records;
  private final long interval;
  private final TimeUnit timeUnit;

  private SpringKafkaesqueConsumer(
      EmbeddedKafkaBroker embeddedKafkaBroker,
      String topic,
      Deserializer<Key> keyDeserializer,
      Deserializer<Value> valueDeserializer,
      long interval,
      TimeUnit timeUnit) {
    this.interval = interval;
    this.timeUnit = timeUnit;
    var consumerProps =
        KafkaTestUtils.consumerProps("kafkaesqueConsumer", "false", embeddedKafkaBroker);
    var cf =
        new DefaultKafkaConsumerFactory<>(
            consumerProps, keyDeserializer, valueDeserializer);
    var containerProperties = new ContainerProperties(topic);
    container = new KafkaMessageListenerContainer<>(cf, containerProperties);
    records = new LinkedBlockingQueue<>();
    container.setupMessageListener((MessageListener<Key, Value>) records::add);
    container.setBeanName("kafkaesqueContainer");
    container.start();
    ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
  }
  
  /**
   * Consumes the messages in the topic. After the the read operation, the consumer remains in a
   * closed state and cannot be used anymore.
   * @return Read messages
   */
  public ConsumedResults<Key, Value> poll() {
    try {
      final Collection<ConsumerRecord<Key, Value>> readMessages = new ArrayList<>();
      Awaitility.await()
          .atMost(interval, timeUnit)
          .until(() -> areNewMessagesRead(readMessages));
      container.stop();
      return new ConsumedResults<>(readMessages);
    } catch (Exception ex) {
      throw new KafkaesqueConsumerPollException("Error during the poll operation", ex);
    }
  }
  
  private Boolean areNewMessagesRead(Collection<ConsumerRecord<Key, Value>> readMessages) {
    final int priorSize = readMessages.size();
    records.drainTo(readMessages);
    return readMessages.size() == priorSize;
  }
  
  public static class Builder<Key, Value>
      implements KafkaesqueConsumer.Builder<EmbeddedKafkaBroker, Key, Value> {

    private String topic;
    private Deserializer<Key> keyDeserializer;
    private Deserializer<Value> valueDeserializer;
    private long interval = 200;
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    private final EmbeddedKafkaBroker embeddedKafkaBroker;

    private Builder(EmbeddedKafkaBroker embeddedKafkaBroker) {
      this.embeddedKafkaBroker = embeddedKafkaBroker;
    }

    public static <Key, Value> Builder<Key, Value> newInstance(
        EmbeddedKafkaBroker embeddedKafkaBroker) {
      return new Builder<>(embeddedKafkaBroker);
    }

    public Builder<Key, Value> fromTopic(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder<Key, Value> withDeserializers(
        Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer) {
      this.keyDeserializer = keyDeserializer;
      this.valueDeserializer = valueDeserializer;
      return this;
    }

    public Builder<Key, Value> waitingAtMost(long interval, TimeUnit unit) {
      this.interval = interval;
      this.timeUnit = unit;
      return this;
    }

    public SpringKafkaesqueConsumer<Key, Value> expecting() {
      validateInputs();
      return new SpringKafkaesqueConsumer<>(
          embeddedKafkaBroker, topic, keyDeserializer, valueDeserializer, interval, timeUnit);
    }

    private void validateInputs() {
      validateEmbeddedKafkaBroker();
      validateTopic();
      validateDeserializers();
    }

    private void validateEmbeddedKafkaBroker() {
      if (embeddedKafkaBroker == null) {
        throw new AssertionError("The embedded kafka broker cannot be null");
      }
    }

    private void validateTopic() {
      if (StringUtils.isEmpty(topic)) {
        throw new AssertionError("The topic name cannot be empty");
      }
    }

    private void validateDeserializers() {
      if (keyDeserializer == null || valueDeserializer == null) {
        throw new AssertionError("The deserializers cannot be null");
      }
    }
  }
}
