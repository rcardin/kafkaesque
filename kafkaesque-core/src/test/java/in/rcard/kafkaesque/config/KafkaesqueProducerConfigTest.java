package in.rcard.kafkaesque.config;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class KafkaesqueProducerConfigTest {

    @Test
  public void shouldLoadKafkaesqueProducerConfigFromGivenClasspathProperties() throws IOException {
    final KafkaesqueConfigLoader kafkaesqueConfigLoader = new TypesafeKafkaesqueConfigLoader();
      KafkaesqueProducerConfig kafkaesqueProducerConfig = kafkaesqueConfigLoader.loadProducerConfig();
    Properties testProps = new Properties();
    try (InputStream testPropsStream =
        getClass().getClassLoader().getResourceAsStream("reference.conf")) {
      testProps.load(testPropsStream);
    }

      assertThat(kafkaesqueProducerConfig.clientId())
        .isEqualTo(testProps.get("kafkaesque.producer.client-id"));
      assertThat(kafkaesqueProducerConfig.acks())
        .isEqualTo(testProps.get("kafkaesque.producer.acks"));
  }
}
