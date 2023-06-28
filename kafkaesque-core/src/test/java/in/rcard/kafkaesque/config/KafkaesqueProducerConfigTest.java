package in.rcard.kafkaesque.config;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

public class KafkaesqueProducerConfigTest {

  @Test
  public void shouldLoadKafkaesqueProducerConfigFromGivenClasspathProperties() {
    final KafkaesqueConfigLoader kafkaesqueConfigLoader = new TypesafeKafkaesqueConfigLoader();
    KafkaesqueProducerConfig kafkaesqueProducerConfig = kafkaesqueConfigLoader.loadProducerConfig();

    final Properties actualProperties = kafkaesqueProducerConfig.toProperties();

    assertThat(actualProperties.get(ProducerConfig.CLIENT_ID_CONFIG)).isEqualTo("kfksq-client-id");
    assertThat(actualProperties.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
  }
}
