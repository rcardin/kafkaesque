package in.rcard.kafkaesque.producer;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaesqueProducerConfigMapper {

    public static Map toProducerProperties(KafkaesqueProducerConfig kProducerConfig) {
        final Map props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kProducerConfig.bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kProducerConfig.clientId());
        props.put(ProducerConfig.RETRIES_CONFIG, kProducerConfig.retries());
        props.put(ProducerConfig.ACKS_CONFIG, kProducerConfig.acks());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, kProducerConfig.batchSize());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kProducerConfig.bufferMemory());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kProducerConfig.compressionType());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kProducerConfig.keySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kProducerConfig.valueSerializer());

        return props;
    }
}
