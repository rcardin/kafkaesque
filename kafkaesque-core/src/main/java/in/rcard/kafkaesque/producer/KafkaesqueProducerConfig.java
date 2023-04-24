package in.rcard.kafkaesque.producer;

import org.aeonbits.owner.Config;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.Sources;

@LoadPolicy(LoadType.MERGE)
@Sources({
        "classpath:${configFile}.properties",
        "system:properties",
        "system:env"
})
public interface KafkaesqueProducerConfig extends Config {

    String KAFKAESQUE_PRODUCER_CONFIG_BOOTSTRAP_SERVERS = "kafkaesque.producer.bootstrap-servers";
    String KAFKAESQUE_PRODUCER_CONFIG_CLIENT_ID = "kafkaesque.producer.client-id";
    String KAFKAESQUE_PRODUCER_CONFIG_RETRIES = "kafkaesque.producer.retries";
    String KAFKAESQUE_PRODUCER_CONFIG_ACKS = "kafkaesque.producer.acks";
    String KAFKAESQUE_PRODUCER_CONFIG_BATCH_SIZE = "kafkaesque.producer.batch-size";
    String KAFKAESQUE_PRODUCER_CONFIG_BUFFER_MEMORY = "kafkaesque.producer.buffer-memory";
    String KAFKAESQUE_PRODUCER_CONFIG_COMPRESSION_TYPE = "kafkaesque.producer.compression-type";
    String KAFKAESQUE_PRODUCER_CONFIG_KEY_SERIALIZER = "kafkaesque.producer.key-serializer";
    String KAFKAESQUE_PRODUCER_CONFIG_VALUE_SERIALIZER = "kafkaesque.producer.value-serializer";

    @Key(KAFKAESQUE_PRODUCER_CONFIG_BOOTSTRAP_SERVERS)
    @DefaultValue("")
    String bootstrapServers();

    @Key(KAFKAESQUE_PRODUCER_CONFIG_CLIENT_ID)
    @DefaultValue("")
    String clientId();

    @Key(KAFKAESQUE_PRODUCER_CONFIG_RETRIES)
    @DefaultValue("2147483647")
    int retries();

    @Key(KAFKAESQUE_PRODUCER_CONFIG_ACKS)
    @DefaultValue("all")
    String acks();

    @Key(KAFKAESQUE_PRODUCER_CONFIG_BATCH_SIZE)
    @DefaultValue("16384")
    int batchSize();

    @Key(KAFKAESQUE_PRODUCER_CONFIG_BUFFER_MEMORY)
    @DefaultValue("33554432")
    String bufferMemory();

    @Key(KAFKAESQUE_PRODUCER_CONFIG_COMPRESSION_TYPE)
    @DefaultValue("none")
    String compressionType();

    @Key(KAFKAESQUE_PRODUCER_CONFIG_KEY_SERIALIZER)
    @DefaultValue("org.apache.kafka.common.serialization.StringSerializer")
    Class keySerializer();

    @Key(KAFKAESQUE_PRODUCER_CONFIG_VALUE_SERIALIZER)
    @DefaultValue("org.apache.kafka.common.serialization.StringSerializer")
    Class valueSerializer();
}
