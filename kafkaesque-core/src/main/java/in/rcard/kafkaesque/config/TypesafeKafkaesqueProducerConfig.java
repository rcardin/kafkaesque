package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;

public class TypesafeKafkaesqueProducerConfig implements KafkaesqueProducerConfig {
    private final String bootstrapServers;
    private final String clientId;
    private final int retries;
    private final String acks;
    private final int batchSize;
    private final long bufferMemory;
    private final String compressionType;

    public TypesafeKafkaesqueProducerConfig(Config config) {
        this.bootstrapServers = config.getString("bootstrap-servers");
        this.clientId = config.getString("client-id");
        this.retries = config.getInt("retries");
        this.acks = config.getString("acks");
        this.batchSize = config.getInt("batch-size");
        this.bufferMemory = config.getLong("buffer-memory");
        this.compressionType = config.getString("compression-type");
    }

    @Override
    public String bootstrapServers() {
        return this.bootstrapServers;
    }

    @Override
    public String clientId() {
        return this.clientId;
    }

    @Override
    public int retries() {
        return this.retries;
    }

    @Override
    public String acks() {
        return this.acks;
    }

    @Override
    public int batchSize() {
        return this.batchSize;
    }

    @Override
    public long bufferMemory() {
        return this.bufferMemory;
    }

    @Override
    public String compressionType() {
        return this.compressionType;
    }
}
