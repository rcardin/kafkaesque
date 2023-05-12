package in.rcard.kafkaesque.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import in.rcard.kafkaesque.config.KafkaesqueConfigLoader;
import in.rcard.kafkaesque.config.KafkaesqueProducerConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class KafkaesqueProducerConfigTest {

    private final String KAFKAESQUE_PRODUCER_CONFIG_CLIENT_ID = "kafkaesque.producer.client-id";
    private final String KAFKAESQUE_PRODUCER_CONFIG_ACKS = "kafkaesque.producer.acks";

    private KafkaesqueProducerConfig kafkaesqueProducerConfig;

    @Test
    public void shouldLoadKafkaesqueProducerConfigFromGivenClasspathProperties() throws IOException {
        kafkaesqueProducerConfig = KafkaesqueConfigLoader.loadProducerConfig();
        Properties testProps = new Properties();
        try (InputStream testPropsStream = getClass()
                .getClassLoader()
                .getResourceAsStream("reference.conf")
        ) {
            testProps.load(testPropsStream);
        }

        assertThat(kafkaesqueProducerConfig.clientId()).isEqualTo(testProps.get(KAFKAESQUE_PRODUCER_CONFIG_CLIENT_ID));
        assertThat(kafkaesqueProducerConfig.acks()).isEqualTo(testProps.get(KAFKAESQUE_PRODUCER_CONFIG_ACKS));
    }
}
