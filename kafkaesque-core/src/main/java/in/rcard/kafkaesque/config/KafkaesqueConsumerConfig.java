package in.rcard.kafkaesque.config;


import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Properties;

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

    default Properties toConsumerProperties() {
      final Properties props = new Properties();

      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId());
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset());
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit());
      props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Math.toIntExact(autoCommitInterval().toMillis()));
      props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId());
      props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Math.toIntExact(fetchMaxWait().toMillis()));
      props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinSize());
      props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, Math.toIntExact(heartbeatInterval().toMillis()));
      props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel());
      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords());

      return props;
    }
}
