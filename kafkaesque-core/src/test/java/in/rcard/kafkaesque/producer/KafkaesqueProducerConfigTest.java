package in.rcard.kafkaesque.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.aeonbits.owner.ConfigFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static in.rcard.kafkaesque.producer.KafkaesqueProducerConfig.KAFKAESQUE_PRODUCER_CONFIG_ACKS;
import static in.rcard.kafkaesque.producer.KafkaesqueProducerConfig.KAFKAESQUE_PRODUCER_CONFIG_CLIENT_ID;

public class KafkaesqueProducerConfigTest {

    private KafkaesqueProducerConfig kafkaesqueProducerConfig;

    @Test
    public void shouldLoadKafkaesqueProducerConfigFromGivenClasspathProperties() throws IOException {
        ConfigFactory.setProperty("configFile", "kafkaesque.test");
        kafkaesqueProducerConfig = ConfigFactory.create(KafkaesqueProducerConfig.class);
        Properties testProps = new Properties();
        try (InputStream testPropsStream = getClass()
                .getClassLoader()
                .getResourceAsStream("kafkaesque.test.properties")
        ) {
            testProps.load(testPropsStream);
        }

        assertThat(kafkaesqueProducerConfig.clientId()).isEqualTo(testProps.get(KAFKAESQUE_PRODUCER_CONFIG_CLIENT_ID));
        assertThat(kafkaesqueProducerConfig.acks()).isEqualTo(testProps.get(KAFKAESQUE_PRODUCER_CONFIG_ACKS));
    }
}
