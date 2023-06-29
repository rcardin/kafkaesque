package in.rcard.kafkaesque.config;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

public class KafkaesqueConsumerConfigTest {

  @Test
  public void shouldLoadKafkaesqueConsumerConfigFromGivenClasspathProperties() {
    final KafkaesqueConfigLoader kafkaesqueConfigLoader = new TypesafeKafkaesqueConfigLoader(null /* TODO */);
    KafkaesqueConsumerConfig kafkaesqueConsumerConfig = kafkaesqueConfigLoader.loadConsumerConfig();

    final Properties actualProperties = kafkaesqueConsumerConfig.toProperties();

    assertThat(actualProperties.get(ConsumerConfig.GROUP_ID_CONFIG))
        .isEqualTo("kfksq-test-consumer");
    assertThat(actualProperties.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("kfksq-client-id");
  }
}
