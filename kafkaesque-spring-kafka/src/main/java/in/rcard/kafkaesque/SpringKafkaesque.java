package in.rcard.kafkaesque;

import in.rcard.kafkaesque.KafkaesqueConsumer.Builder;
import in.rcard.kafkaesque.KafkaesqueConsumer.KafkaesqueConsumerDelegate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

public class SpringKafkaesque implements Kafkaesque {

  private final EmbeddedKafkaBroker embeddedKafkaBroker;

  public SpringKafkaesque(EmbeddedKafkaBroker embeddedKafkaBroker) {
    this.embeddedKafkaBroker = embeddedKafkaBroker;
  }

  @Override
  public <Key, Value> Builder<Key, Value> consume() {
    return Builder.newInstance(
        creationInfo -> {
          Map<String, Object> consumerProps =
              KafkaTestUtils.consumerProps("kafkaesqueConsumer", "false", embeddedKafkaBroker);
          DefaultKafkaConsumerFactory<Key, Value> cf =
              new DefaultKafkaConsumerFactory<>(
                  consumerProps,
                  creationInfo.getKeyDeserializer(),
                  creationInfo.getValueDeserializer());
          ContainerProperties containerProperties =
              new ContainerProperties(creationInfo.getTopic());
          KafkaMessageListenerContainer<Key, Value> container =
              new KafkaMessageListenerContainer<>(cf, containerProperties);
          BlockingQueue<ConsumerRecord<Key, Value>> records = new LinkedBlockingQueue<>();
          container.setupMessageListener((MessageListener<Key, Value>) records::add);
          container.setBeanName("kafkaesqueContainer");
          container.start();
          ContainerTestUtils.waitForAssignment(
              container, embeddedKafkaBroker.getPartitionsPerTopic());

          return new KafkaesqueConsumerDelegate<Key, Value>() {
            @Override
            public List<ConsumerRecord<Key, Value>> poll() {
              final List<ConsumerRecord<Key, Value>> newMessages = new ArrayList<>();
              records.drainTo(newMessages);
              return newMessages;
            }
  
            @Override
            public void close() {
              container.stop();
            }
          };
        });
  }

  @Override
  public <Key, Value> KafkaesqueProducer.Builder<Key, Value> produce() {
    return null;
  }
}
