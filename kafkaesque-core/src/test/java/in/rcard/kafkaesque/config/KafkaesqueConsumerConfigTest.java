package in.rcard.kafkaesque.config;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class KafkaesqueConsumerConfigTest {

    @Test
    public void shouldLoadKafkaesqueConsumerConfigFromGivenClasspathProperties() throws IOException {
        final KafkaesqueConfigLoader kafkaesqueConfigLoader = new TypesafeKafkaesqueConfigLoader();
        KafkaesqueConsumerConfig kafkaesqueConsumerConfig = kafkaesqueConfigLoader.loadConsumerConfig();
        Properties testProps = new Properties();
        try (InputStream testPropsStream = getClass()
                .getClassLoader()
                .getResourceAsStream("reference.conf")
        ) {
            testProps.load(testPropsStream);
        }

        assertThat(kafkaesqueConsumerConfig.groupId()).isEqualTo(testProps.get("kafkaesque.consumer.group-id"));
        assertThat(kafkaesqueConsumerConfig.clientId()).isEqualTo(testProps.get("kafkaesque.consumer.client-id"));
    }
}
