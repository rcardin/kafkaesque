package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;

public class KafkaesqueProducerConfigImpl implements KafkaesqueProducerConfig {
    private String bootstrapServers;
    private String clientId;
    private int retries;
    private String acks;
    private int batchSize;
    private long bufferMemory;
    private String compressionType;

    public KafkaesqueProducerConfigImpl(Config config) {
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
