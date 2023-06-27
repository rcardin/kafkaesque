package in.rcard.kafkaesque.config;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public interface KafkaesqueProducerConfig {

    String bootstrapServers();

    String clientId();

    int retries();

    String acks();

    int batchSize();

    long bufferMemory();

    String compressionType();

    default Properties toProducerProperties() {
      final Properties props = new Properties();

      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
      props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId());
      props.put(ProducerConfig.RETRIES_CONFIG, retries());
      props.put(ProducerConfig.ACKS_CONFIG, acks());
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize());
      props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory());
      props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType());

      return props;
    }
}
