package in.rcard.kafkaesque.config;

public interface KafkaesqueProducerConfig {

    String bootstrapServers();

    String clientId();

    int retries();

    String acks();

    int batchSize();

    String bufferMemory();

    String compressionType();
}
