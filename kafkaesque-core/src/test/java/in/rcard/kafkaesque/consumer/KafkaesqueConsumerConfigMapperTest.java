package in.rcard.kafkaesque.consumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static in.rcard.kafkaesque.consumer.KafkaesqueConsumerConfig.*;

public class KafkaesqueConsumerConfigMapperTest {

    private final Map kConsumerConfig = Map.ofEntries(
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_GROUP_ID, "group-id"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_CLIENT_ID, "client-id"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_BOOTSTRAP_SERVERS, "localhost"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_AUTO_OFFSET_RESET, "latest"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_AUTO_COMMIT_INTERVAL, "3000ms"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_ENABLE_AUTO_COMMIT, "false"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_FETCH_MAX_WAIT, "300ms"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_FETCH_MIN_SIZE, "2"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_HEARTBEAT_INTERVAL, "6000ms"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_ISOLATION_LEVEL, "read_committed"),
            Map.entry(KAFKAESQUE_CONSUMER_CONFIG_MAX_POLL_RECORDS, "250")
            );
    @Test
    public void shouldCorrectlyMapKafkaesqueConsumerConfigToConsumerProperties() {
        final KafkaesqueConsumerConfig kafkaesqueConsumerConfig = ConfigFactory.create(KafkaesqueConsumerConfig.class, kConsumerConfig);
        final Map consumerProps = KafkaesqueConsumerConfigMapper.toConsumerProperties(kafkaesqueConsumerConfig);

        assertEquals(consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG), "group-id");
        assertEquals(consumerProps.get(ConsumerConfig.CLIENT_ID_CONFIG), "client-id");
        assertEquals(consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), "localhost");
        assertEquals(consumerProps.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), StringDeserializer.class);
        assertEquals(consumerProps.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), StringDeserializer.class);
        assertEquals(consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG), "group-id");
        assertEquals(consumerProps.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), OffsetResetStrategy.LATEST.name().toLowerCase());
        assertEquals(consumerProps.get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG), Math.toIntExact(Duration.ofSeconds(3).toMillis()));
        assertEquals(consumerProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), false);
        assertEquals(consumerProps.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG), Math.toIntExact(Duration.ofMillis(300).toMillis()));
        assertEquals(consumerProps.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG), 2);
        assertEquals(consumerProps.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), Math.toIntExact(Duration.of(6, ChronoUnit.SECONDS).toMillis()));
        assertEquals(consumerProps.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG), IsolationLevel.READ_COMMITTED.name().toLowerCase());
        assertEquals(consumerProps.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 250);

    }
}
