package in.rcard.kafkaesque;

import in.rcard.kafkaesque.KafkaesqueConsumer.Builder;
import in.rcard.kafkaesque.KafkaesqueConsumer.KafkaesqueConsumerDelegate;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class LiveKafkaesque implements Kafkaesque {
  
  private final String brokersUrl;
  
  public LiveKafkaesque(String brokersUrl) {
    this.brokersUrl = brokersUrl;
  }
  
  @Override
  public <Key, Value> Builder<Key, Value> consume() {
    return Builder.newInstance(
        creationInfo -> {
          
          try {
            final Properties props = new Properties();
            props.put("client.id", InetAddress.getLocalHost().getHostName());
            props.put("group.id", "kafkaesque-consumer");
            props.put("bootstrap.servers", brokersUrl);
            final KafkaConsumer<Key, Value> kafkaConsumer = new KafkaConsumer<>(props);
            kafkaConsumer.subscribe(List.of(creationInfo.getTopic()));
  
            return new KafkaesqueConsumerDelegate<Key, Value>() {
              @Override
              public List<ConsumerRecord<Key, Value>> poll() {
                return null;
              }
    
              @Override
              public void close() {
                // TODO Use a countdown latch?
                kafkaConsumer.close();
              }
            };
            
          } catch (UnknownHostException e) {
            // TODO
          }
        });
  }
  
  @Override
  public <Key, Value> KafkaesqueProducer.Builder<Key, Value> produce() {
    return null;
  }
}
