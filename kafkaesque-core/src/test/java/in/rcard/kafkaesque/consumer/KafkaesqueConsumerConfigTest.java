package in.rcard.kafkaesque.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.aeonbits.owner.ConfigFactory;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static in.rcard.kafkaesque.consumer.KafkaesqueConsumerConfig.KAFKAESQUE_CONSUMER_CONFIG_CLIENT_ID;
import static in.rcard.kafkaesque.consumer.KafkaesqueConsumerConfig.KAFKAESQUE_CONSUMER_CONFIG_GROUP_ID;


public class KafkaesqueConsumerConfigTest {

    private KafkaesqueConsumerConfig kafkaesqueConsumerConfig;

    @Test
    public void shouldLoadKafkaesqueConsumerConfigFromGivenClasspathProperties() throws IOException {
        ConfigFactory.setProperty("configFile", "kafkaesque.test");
        kafkaesqueConsumerConfig = ConfigFactory.create(KafkaesqueConsumerConfig.class);
        Properties testProps = new Properties();
        try (InputStream testPropsStream = getClass()
                .getClassLoader()
                .getResourceAsStream("kafkaesque.test.properties")
        ) {
            testProps.load(testPropsStream);
        }

        assertThat(kafkaesqueConsumerConfig.groupId()).isEqualTo(testProps.get(KAFKAESQUE_CONSUMER_CONFIG_GROUP_ID));
        assertThat(kafkaesqueConsumerConfig.clientId()).isEqualTo(testProps.get(KAFKAESQUE_CONSUMER_CONFIG_CLIENT_ID));
    }
}
