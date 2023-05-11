package in.rcard.kafkaesque.config;

public class KafkaesqueProducerConfigImpl implements KafkaesqueProducerConfig {
    private String bootstrapServers;
    private String clientId;
    private int retries;
    private String acks;
    private int batchSize;
    private String bufferMemory;
    private String compressionType;

    public KafkaesqueProducerConfigImpl() {
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(String bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    @Override
    public String bootstrapServers() {
        return getBootstrapServers();
    }

    @Override
    public String clientId() {
        return getClientId();
    }

    @Override
    public int retries() {
        return getRetries();
    }

    @Override
    public String acks() {
        return getAcks();
    }

    @Override
    public int batchSize() {
        return getBatchSize();
    }

    @Override
    public String bufferMemory() {
        return getBufferMemory();
    }

    @Override
    public String compressionType() {
        return getCompressionType();
    }
}
