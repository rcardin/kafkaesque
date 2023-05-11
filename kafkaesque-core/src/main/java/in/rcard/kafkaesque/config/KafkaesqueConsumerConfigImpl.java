package in.rcard.kafkaesque.config;

import java.time.Duration;

public class KafkaesqueConsumerConfigImpl implements KafkaesqueConsumerConfig {

    private String groupId;
    private String autoOffsetReset;
    private String bootstrapServers;
    private boolean enableAutoCommit;
    private Duration autoCommitInterval;
    private String clientId;
    private Duration fetchMaxWait;
    private int fetchMinSize;
    private Duration heartbeatInterval;
    private String isolationLevel;
    private int maxPollRecords;

    public KafkaesqueConsumerConfigImpl() {}

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public Duration getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public void setAutoCommitInterval(Duration autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Duration getFetchMaxWait() {
        return fetchMaxWait;
    }

    public void setFetchMaxWait(Duration fetchMaxWait) {
        this.fetchMaxWait = fetchMaxWait;
    }

    public int getFetchMinSize() {
        return fetchMinSize;
    }

    public void setFetchMinSize(int fetchMinSize) {
        this.fetchMinSize = fetchMinSize;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public String getIsolationLevel() {
        return isolationLevel;
    }

    public void setIsolationLevel(String isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    @Override
    public String groupId() {
        return getGroupId();
    }

    @Override
    public String autoOffsetReset() {
        return getAutoOffsetReset();
    }

    @Override
    public String bootstrapServers() {
        return getBootstrapServers();
    }

    @Override
    public boolean enableAutoCommit() {
        return isEnableAutoCommit();
    }

    @Override
    public Duration autoCommitInterval() {
        return getAutoCommitInterval();
    }

    @Override
    public String clientId() {
        return getClientId();
    }

    @Override
    public Duration fetchMaxWait() {
        return getFetchMaxWait();
    }

    @Override
    public int fetchMinSize() {
        return getFetchMinSize();
    }

    @Override
    public Duration heartbeatInterval() {
        return getHeartbeatInterval();
    }

    @Override
    public String isolationLevel() {
        return getIsolationLevel();
    }

    @Override
    public int maxPollRecords() {
        return getMaxPollRecords();
    }
}
