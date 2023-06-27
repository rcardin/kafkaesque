package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;

import java.time.Duration;

public class TypesafeKafkaesqueConsumerConfig implements KafkaesqueConsumerConfig {

    private final String groupId;
    private final String autoOffsetReset;
    private final String bootstrapServers;
    private final boolean enableAutoCommit;
    private final Duration autoCommitInterval;
    private final String clientId;
    private final Duration fetchMaxWait;
    private final int fetchMinSize;
    private final Duration heartbeatInterval;
    private final String isolationLevel;
    private final int maxPollRecords;

    public TypesafeKafkaesqueConsumerConfig(Config config) {
        this.groupId = config.getString("group-id");
        this.autoOffsetReset = config.getString("auto-offset-reset");
        this.bootstrapServers = config.getString("bootstrap-servers");
        this.enableAutoCommit = config.getBoolean("enable-auto-commit");
        this.autoCommitInterval = config.getDuration("auto-commit-interval");
        this.clientId = config.getString("client-id");
        this.fetchMaxWait = config.getDuration("fetch-max-wait");
        this.fetchMinSize = config.getInt("fetch-min-size");
        this.heartbeatInterval = config.getDuration("heartbeat-interval");
        this.isolationLevel = config.getString("isolation-level");
        this.maxPollRecords = config.getInt("max-poll-records");
    }

    @Override
    public String groupId() {
        return this.groupId;
    }

    @Override
    public String autoOffsetReset() {
        return this.autoOffsetReset;
    }

    @Override
    public String bootstrapServers() {
        return this.bootstrapServers;
    }

    @Override
    public boolean enableAutoCommit() {
        return this.enableAutoCommit;
    }

    @Override
    public Duration autoCommitInterval() {
        return this.autoCommitInterval;
    }

    @Override
    public String clientId() {
        return this.clientId;
    }

    @Override
    public Duration fetchMaxWait() {
        return this.fetchMaxWait;
    }

    @Override
    public int fetchMinSize() {
        return this.fetchMinSize;
    }

    @Override
    public Duration heartbeatInterval() {
        return this.heartbeatInterval;
    }

    @Override
    public String isolationLevel() {
        return this.isolationLevel;
    }

    @Override
    public int maxPollRecords() {
        return this.maxPollRecords;
    }
}
