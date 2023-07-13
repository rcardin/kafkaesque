package in.rcard.kafkaesque.config;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

public class KafkaesqueConsumerConfigTest {

  @Test
  public void shouldLoadKafkaesqueConsumerConfigInPropertiesFormatFromGivenClasspath() {
    final KafkaesqueConfigLoader kafkaesqueConfigLoader =
        new TypesafeKafkaesqueConfigLoader("reference.properties");
    KafkaesqueConsumerConfig kafkaesqueConsumerConfig = kafkaesqueConfigLoader.loadConsumerConfig();

    final Properties actualProperties = kafkaesqueConsumerConfig.toProperties();

    assertThat(actualProperties.get(ConsumerConfig.GROUP_ID_CONFIG))
        .isEqualTo("kfksq-test-consumer");
    assertThat(actualProperties.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("kfksq-client-id");
  }

  @Test
  public void shouldLoadKafkaesqueConsumerConfigInHoconFormatFromGivenClasspath() {
    final KafkaesqueConfigLoader kafkaesqueConfigLoader =
        new TypesafeKafkaesqueConfigLoader("reference.conf");
    KafkaesqueConsumerConfig kafkaesqueConsumerConfig = kafkaesqueConfigLoader.loadConsumerConfig();

    final Properties actualProperties = kafkaesqueConsumerConfig.toProperties();

    assertThat(actualProperties.get(ConsumerConfig.GROUP_ID_CONFIG))
        .isEqualTo("kfksq-test-consumer");
    assertThat(actualProperties.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("kfksq-client-id");
  }

  @Test
  public void shouldLoadKafkaesqueConsumerConfigInJsonFormatFromGivenClasspath() {
    final KafkaesqueConfigLoader kafkaesqueConfigLoader =
            new TypesafeKafkaesqueConfigLoader("reference.json");
    KafkaesqueConsumerConfig kafkaesqueConsumerConfig = kafkaesqueConfigLoader.loadConsumerConfig();

    final Properties actualProperties = kafkaesqueConsumerConfig.toProperties();

    assertThat(actualProperties.get(ConsumerConfig.GROUP_ID_CONFIG))
            .isEqualTo("kfksq-test-consumer");
    assertThat(actualProperties.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("kfksq-client-id");
  }
}
