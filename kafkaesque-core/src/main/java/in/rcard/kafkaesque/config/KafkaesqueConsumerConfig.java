package in.rcard.kafkaesque.config;


import java.time.Duration;

public interface KafkaesqueConsumerConfig {

    String groupId();

    String autoOffsetReset();

    String bootstrapServers();

    boolean enableAutoCommit();

    Duration autoCommitInterval();

    String clientId();

    Duration fetchMaxWait();

    int fetchMinSize();

    Duration heartbeatInterval();

    String isolationLevel();

    int maxPollRecords();
}
