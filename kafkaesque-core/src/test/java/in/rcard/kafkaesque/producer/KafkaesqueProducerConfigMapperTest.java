package in.rcard.kafkaesque.producer;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.aeonbits.owner.ConfigFactory;

import static in.rcard.kafkaesque.producer.KafkaesqueProducerConfig.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaesqueProducerConfigMapperTest {
    private final Map kProducerConfig = Map.ofEntries(
            Map.entry(KAFKAESQUE_PRODUCER_CONFIG_BOOTSTRAP_SERVERS, "localhost"),
            Map.entry(KAFKAESQUE_PRODUCER_CONFIG_CLIENT_ID, "client-id"),
            Map.entry(KAFKAESQUE_PRODUCER_CONFIG_RETRIES, "3"),
            Map.entry(KAFKAESQUE_PRODUCER_CONFIG_ACKS, "all"),
            Map.entry(KAFKAESQUE_PRODUCER_CONFIG_BATCH_SIZE, "25"),
            Map.entry(KAFKAESQUE_PRODUCER_CONFIG_BUFFER_MEMORY, "1024"),
            Map.entry(KAFKAESQUE_PRODUCER_CONFIG_COMPRESSION_TYPE, "gzip"),
            Map.entry(KAFKAESQUE_PRODUCER_CONFIG_KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer"),
            Map.entry(KAFKAESQUE_PRODUCER_CONFIG_VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer")
    );
    @Test
    public void shouldCorrectlyMapKafkaesqueProducerConfigToProducerProperties() {
        final KafkaesqueProducerConfig kafkaesqueProducerConfig = ConfigFactory.create(KafkaesqueProducerConfig.class, kProducerConfig);
        final Map producerProps = KafkaesqueProducerConfigMapper.toProducerProperties(kafkaesqueProducerConfig);

        assertEquals(producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), "localhost");
        assertEquals(producerProps.get(ProducerConfig.CLIENT_ID_CONFIG), "client-id");
        assertEquals(producerProps.get(ProducerConfig.RETRIES_CONFIG), 3);
        assertEquals(producerProps.get(ProducerConfig.ACKS_CONFIG), "all");
        assertEquals(producerProps.get(ProducerConfig.BATCH_SIZE_CONFIG), 25);
        assertEquals(producerProps.get(ProducerConfig.BUFFER_MEMORY_CONFIG), "1024");
        assertEquals(producerProps.get(ProducerConfig.COMPRESSION_TYPE_CONFIG), "gzip");
        assertEquals(producerProps.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), StringSerializer.class);
        assertEquals(producerProps.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), StringSerializer.class);
    }
}
