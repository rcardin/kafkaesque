package in.rcard.kafkaesque.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import in.rcard.kafkaesque.config.KafkaesqueConfigLoader;
import in.rcard.kafkaesque.config.KafkaesqueConsumerConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


public class KafkaesqueConsumerConfigTest {

    private final String KAFKAESQUE_CONSUMER_CONFIG_GROUP_ID = "kafkaesque.consumer.group-id";
    private final String KAFKAESQUE_CONSUMER_CONFIG_CLIENT_ID = "kafkaesque.consumer.client-id";
    private KafkaesqueConsumerConfig kafkaesqueConsumerConfig;

    @Test
    public void shouldLoadKafkaesqueConsumerConfigFromGivenClasspathProperties() throws IOException {
        kafkaesqueConsumerConfig = KafkaesqueConfigLoader.loadConsumerConfig();
        Properties testProps = new Properties();
        try (InputStream testPropsStream = getClass()
                .getClassLoader()
                .getResourceAsStream("reference.conf")
        ) {
            testProps.load(testPropsStream);
        }

        assertThat(kafkaesqueConsumerConfig.groupId()).isEqualTo(testProps.get(KAFKAESQUE_CONSUMER_CONFIG_GROUP_ID));
        assertThat(kafkaesqueConsumerConfig.clientId()).isEqualTo(testProps.get(KAFKAESQUE_CONSUMER_CONFIG_CLIENT_ID));
    }
}
