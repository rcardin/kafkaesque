package in.rcard.kafkaesque.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import in.rcard.kafkaesque.config.KafkaesqueConfigLoader;
import in.rcard.kafkaesque.config.KafkaesqueConsumerConfig;
import in.rcard.kafkaesque.config.TypesafeKafkaesqueConfigLoader;
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
 * @see KafkaesqueConsumerDSL
 */
public class KafkaesqueConsumer<Key, Value> {

  private final KafkaConsumer<Key, Value> kafkaConsumer;

  private final long interval;
  private final TimeUnit timeUnit;
  private final int emptyPollsCount;
  private final long emptyPollsInterval;
  private final TimeUnit emptyPollsTimeUnit;

  private final DelegateCreationInfo<Key, Value> creationInfo;

  KafkaesqueConsumer(
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
    Properties creationProps = new Properties();
    creationProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaesque-consumer");
    creationProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    creationProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersUrl);
    creationProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    creationProps.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, creationInfo.getKeyDeserializer().getClass());
    creationProps.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            creationInfo.getValueDeserializer().getClass());

    // TODO Move this to the config loader
    final KafkaesqueConfigLoader kafkaesqueConfigLoader = new TypesafeKafkaesqueConfigLoader();

    final KafkaesqueConsumerConfig consumerConfig = kafkaesqueConfigLoader.loadConsumerConfig();
    final Properties consumerProperties = consumerConfig.toProperties();
    consumerProperties.putAll(creationProps);
    final KafkaConsumer<Key, Value> consumer = new KafkaConsumer<>(consumerProperties);
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
//            System.out.println("Assigned");
          }
        });
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .until(
            () -> {
              // The actual assignment of a topic to a consumer is done after a while
              // the consumer starts to poll messages. So, we forced the consumer to poll
              // from the topic, and we wait until the consumer is assigned to the topic.
              try {
                consumer.poll(Duration.ofMillis(100));
              } catch (Exception e) {
                // Ignore every exception during polling. We are only interested in the assignment
              }
              final boolean assigned = latch.getCount() == 0;
              if (assigned) {
//                System.out.println("Resetting the offset to beginning");
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
  AssertionsOnConsumedDelegate<Key, Value> poll() {
    final List<ConsumerRecord<Key, Value>> readMessages = new ArrayList<>();
    try {
      final AtomicInteger emptyCycles = new AtomicInteger(emptyPollsCount);
//      System.out.println("Empty cycles to await: " + emptyPollsCount);
      Awaitility.await()
          .atMost(interval, timeUnit)
          .pollInterval(emptyPollsInterval, emptyPollsTimeUnit)
          .until(
              () -> {
                if (isEmptyPollAfterSomeMessagesWereRead(readMessages)) {
                  final int remainingCycles = emptyCycles.decrementAndGet();
//                  System.out.println("Remaining empty cycles: " + remainingCycles);
//                  System.out.println(System.currentTimeMillis());
                  return remainingCycles == 0;
                }
                return false;
              });
      return new AssertionsOnConsumedDelegate<>(new AssertionsOnConsumed<>(readMessages), this);
    } catch (ConditionTimeoutException ctex) {
      if (readMessages.isEmpty()) {
        throw new AssertionError(
            String.format(
                "The consumer cannot find any message during the given time interval: %d %s",
                interval, timeUnit.toString()));
      } else {
        throw new AssertionError(
            String.format(
                "The consumer reads new messages until the end of the given time interval: %d %s",
                interval, timeUnit.toString()));
      }
    } catch (Exception ex) {
      throw new KafkaesqueConsumerPollException("Error during the poll operation", ex);
    }
  }
  
  private boolean isEmptyPollAfterSomeMessagesWereRead(List<ConsumerRecord<Key, Value>> readMessages) {
    return  readNewMessages(readMessages) == 0 && !readMessages.isEmpty();
  }
  
  private int readNewMessages(List<ConsumerRecord<Key, Value>> readMessages) {
    final ConsumerRecords<Key, Value> polled = kafkaConsumer.poll(Duration.ofMillis(50L));
//    System.out.println("Polled: " + polled.count());
    final List<ConsumerRecord<Key, Value>> newMessages = new ArrayList<>();
    polled.records(creationInfo.getTopic()).forEach(newMessages::add);
    if (!newMessages.isEmpty()) {
      readMessages.addAll(newMessages);
    }
    return newMessages.size();
  }

  /**
   * Closes the consumer. After the closing operation, the consumer cannot read any more messages.
   */
  public void close() {
    kafkaConsumer.close();
  }

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
  static class DelegateCreationInfo<Key, Value> {
    private final String topic;
    private final Deserializer<Key> keyDeserializer;
    private final Deserializer<Value> valueDeserializer;

    DelegateCreationInfo(
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
