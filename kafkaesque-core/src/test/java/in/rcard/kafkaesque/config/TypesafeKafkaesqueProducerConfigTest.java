package in.rcard.kafkaesque.config;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

public class TypesafeKafkaesqueProducerConfigTest {

  @Test
  public void shouldLoadKafkaesqueProducerConfigInPropertiesFormatFromGivenClasspath() {
    final KafkaesqueConfigLoader kafkaesqueConfigLoader =
        new TypesafeKafkaesqueConfigLoader("reference.properties");
    KafkaesqueProducerConfig kafkaesqueProducerConfig = kafkaesqueConfigLoader.loadProducerConfig();

    final Properties actualProperties = kafkaesqueProducerConfig.toProperties();

    assertThat(actualProperties.get(ProducerConfig.CLIENT_ID_CONFIG)).isEqualTo("kfksq-client-id");
    assertThat(actualProperties.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
  }

  @Test
  public void shouldLoadKafkaesqueProducerConfigInHoconFormatFromGivenClasspath() {
    final KafkaesqueConfigLoader kafkaesqueConfigLoader =
        new TypesafeKafkaesqueConfigLoader("reference.conf");
    KafkaesqueProducerConfig kafkaesqueProducerConfig = kafkaesqueConfigLoader.loadProducerConfig();

    final Properties actualProperties = kafkaesqueProducerConfig.toProperties();

    assertThat(actualProperties.get(ProducerConfig.CLIENT_ID_CONFIG)).isEqualTo("kfksq-client-id");
    assertThat(actualProperties.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
  }

  @Test
  public void shouldLoadKafkaesqueProducerConfigInJsonFormatFromGivenClasspath() {
    final KafkaesqueConfigLoader kafkaesqueConfigLoader =
        new TypesafeKafkaesqueConfigLoader("reference.json");
    KafkaesqueProducerConfig kafkaesqueProducerConfig = kafkaesqueConfigLoader.loadProducerConfig();

    final Properties actualProperties = kafkaesqueProducerConfig.toProperties();

    assertThat(actualProperties.get(ProducerConfig.CLIENT_ID_CONFIG)).isEqualTo("kfksq-client-id");
    assertThat(actualProperties.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
  }

  @Test
  void shouldNotLoadAbsentConsumerProperties() {
    final KafkaesqueConfigLoader kafkaesqueConfigLoader =
        new TypesafeKafkaesqueConfigLoader("empty.properties");
    KafkaesqueProducerConfig kafkaesqueProducerConfig = kafkaesqueConfigLoader.loadProducerConfig();

    final Properties actualProperties = kafkaesqueProducerConfig.toProperties();

    assertThat(actualProperties.isEmpty()).isTrue();
  }
}
