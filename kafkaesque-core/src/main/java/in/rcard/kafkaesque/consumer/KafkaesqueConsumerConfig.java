package in.rcard.kafkaesque.consumer;

import java.time.Duration;

import org.aeonbits.owner.Config;
import org.aeonbits.owner.Config.Sources;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.converters.DurationConverter;



@LoadPolicy(Config.LoadType.MERGE)
@Sources({
        "classpath:${configFile}.properties",
        "system:properties",
        "system:env"
})
public interface KafkaesqueConsumerConfig extends Config {

    String KAFKAESQUE_CONSUMER_CONFIG_GROUP_ID = "kafkaesque.consumer.group-id";
    String KAFKAESQUE_CONSUMER_CONFIG_CLIENT_ID = "kafkaesque.consumer.client-id";
    String KAFKAESQUE_CONSUMER_CONFIG_BOOTSTRAP_SERVERS = "kafkaesque.consumer.bootstrap-servers";
    String KAFKAESQUE_CONSUMER_CONFIG_KEY_DESERIALIZER = "kafkaesque.consumer.key-deserializer";
    String KAFKAESQUE_CONSUMER_CONFIG_VALUE_DESERIALIZER = "kafkaesque.consumer.value-deserializer";
    String KAFKAESQUE_CONSUMER_CONFIG_AUTO_OFFSET_RESET = "kafkaesque.consumer.auto-offset-reset";
    String KAFKAESQUE_CONSUMER_CONFIG_AUTO_COMMIT_INTERVAL = "kafkaesque.consumer.auto-commit-interval";
    String KAFKAESQUE_CONSUMER_CONFIG_ENABLE_AUTO_COMMIT = "kafkaesque.consumer.enable-auto-commit";
    String KAFKAESQUE_CONSUMER_CONFIG_FETCH_MAX_WAIT = "kafkaesque.consumer.fetch-max-wait";
    String KAFKAESQUE_CONSUMER_CONFIG_FETCH_MIN_SIZE = "kafkaesque.consumer.fetch-min-size";
    String KAFKAESQUE_CONSUMER_CONFIG_HEARTBEAT_INTERVAL = "kafkaesque.consumer.heartbeat-interval";
    String KAFKAESQUE_CONSUMER_CONFIG_ISOLATION_LEVEL = "kafkaesque.consumer.isolation-level";
    String KAFKAESQUE_CONSUMER_CONFIG_MAX_POLL_RECORDS = "kafkaesque.consumer.max-poll-records";

    @Key(KAFKAESQUE_CONSUMER_CONFIG_GROUP_ID)
    @DefaultValue("kafkaesque-consumer")
    String groupId();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_AUTO_OFFSET_RESET)
    @DefaultValue("earliest")
    String autoOffsetReset();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_BOOTSTRAP_SERVERS)
    String bootstrapServers();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_AUTO_COMMIT_INTERVAL)
    @DefaultValue("5000")
    @ConverterClass(DurationConverter.class)
    Duration autoCommitInterval();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_ENABLE_AUTO_COMMIT)
    @DefaultValue("false")
    boolean enableAutoCommit();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_KEY_DESERIALIZER)
    @DefaultValue("org.apache.kafka.common.serialization.StringDeserializer")
    Class keyDeserializer();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_VALUE_DESERIALIZER)
    @DefaultValue("org.apache.kafka.common.serialization.StringDeserializer")
    Class valueDeserializer();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_CLIENT_ID)
    @DefaultValue("")
    String clientId();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_FETCH_MAX_WAIT)
    @DefaultValue("500")
    @ConverterClass(DurationConverter.class)
    Duration fetchMaxWait();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_FETCH_MIN_SIZE)
    @DefaultValue("1")
    int fetchMinSize();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_HEARTBEAT_INTERVAL)
    @DefaultValue("3000")
    @ConverterClass(DurationConverter.class)
    Duration heartbeatInterval();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_ISOLATION_LEVEL)
    @DefaultValue("read_uncommitted")
    String isolationLevel();

    @Key(KAFKAESQUE_CONSUMER_CONFIG_MAX_POLL_RECORDS)
    @DefaultValue("500")
    int maxPollRecords();
}
