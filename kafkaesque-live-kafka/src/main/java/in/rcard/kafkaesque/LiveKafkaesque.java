package in.rcard.kafkaesque;

import in.rcard.kafkaesque.KafkaesqueConsumer.Builder;
import in.rcard.kafkaesque.KafkaesqueConsumer.KafkaesqueConsumerDelegate;
import in.rcard.kafkaesque.KafkaesqueConsumer.KafkaesqueConsumerDelegate.DelegateCreationInfo;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;

public class LiveKafkaesque implements Kafkaesque {

  private final String brokersUrl;

  public LiveKafkaesque(String brokersUrl) {
    this.brokersUrl = brokersUrl;
  }

  @Override
  public <Key, Value> Builder<Key, Value> consume() {
    return Builder.newInstance(
        new Function<DelegateCreationInfo<Key, Value>, KafkaesqueConsumerDelegate<Key, Value>>() {
          @Override
          public KafkaesqueConsumerDelegate<Key, Value> apply(
              DelegateCreationInfo<Key, Value> creationInfo) {
            final Properties props = new Properties();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaesque-consumer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersUrl);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                creationInfo.getKeyDeserializer().getClass());
            props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                creationInfo.getValueDeserializer().getClass());
            final KafkaConsumer<Key, Value> consumer = new KafkaConsumer<>(props);
            subscribeConsumerToTopic(consumer, creationInfo.getTopic());
  
            return new KafkaesqueConsumerDelegate<>() {
              @Override
              public List<ConsumerRecord<Key, Value>> poll() {
                final ConsumerRecords<Key, Value> polled = consumer.poll(Duration.ofMillis(50L));
                final List<ConsumerRecord<Key, Value>> records = new ArrayList<>();
                polled.records(creationInfo.getTopic()).forEach(records::add);
                return records;
              }

              @Override
              public void close() {
                consumer.close();
              }
            };
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
//                    System.out.println("Assigned");
                  }
                });
            Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .until(
                    () -> {
                      // The actual assignment of a topic to a consumer is done after a while
                      // the consumer starts to poll messages. So, we forced the consumer to poll
                      // from the topic and we wait until the consumer is assigned to the topic.
                      consumer.poll(Duration.ofMillis(100));
                      return latch.getCount() == 0;
                    });
          }
        });
  }

  @Override
  public <Key, Value> KafkaesqueProducer.Builder<Key, Value> produce() {
    return new KafkaesqueProducer.Builder<>(
        creationInfo -> {
          final Properties props = new Properties();
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersUrl);
          props.put(ProducerConfig.ACKS_CONFIG, "all");
          props.put(
              ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              creationInfo.getKeySerializer().getClass());
          props.put(
              ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              creationInfo.getValueSerializer().getClass());
          Producer<Key, Value> producer = new KafkaProducer<>(props);
          return record -> {
            CompletableFuture<RecordMetadata> promiseOnMetadata = new CompletableFuture<>();
            producer.send(
                record,
                (metadata, exception) -> {
                  if (exception == null) {
                    promiseOnMetadata.complete(metadata);
                  } else {
                    promiseOnMetadata.completeExceptionally(exception);
                  }
                });
            return promiseOnMetadata;
          };
        });
  }
}
