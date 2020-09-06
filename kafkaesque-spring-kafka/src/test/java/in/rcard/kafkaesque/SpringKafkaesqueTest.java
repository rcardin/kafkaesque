package in.rcard.kafkaesque;

import in.rcard.kafkaesque.SpringKafkaesqueTest.TestConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest(classes = { TestConfiguration.class })
@EmbeddedKafka(
    partitions = 1,
    topics = { "test" }
)
class SpringKafkaesqueTest {
  
  @Autowired
  private EmbeddedKafkaBroker broker;
  
  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;
  
  @Test
  void consume() {
    System.out.println("Here I am!");
    // TODO
  }
  
  @Test
  void produce() {
    // TODO
  }
  
  @Configuration
  static class TestConfiguration {
    @Bean
    ProducerFactory<String, String> producerFactory() {
      return new DefaultKafkaProducerFactory<>(producerConfigs());
    }
  
    @Bean
    Map<String, Object> producerConfigs() {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      return props;
    }
  
    @Bean
    KafkaTemplate<String, String> kafkaTemplate() {
      return new KafkaTemplate<>(producerFactory());
    }
  }
}