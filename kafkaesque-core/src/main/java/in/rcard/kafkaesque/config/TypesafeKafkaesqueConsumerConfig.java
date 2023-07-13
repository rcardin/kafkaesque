package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class TypesafeKafkaesqueConsumerConfig implements KafkaesqueConsumerConfig {

  private final Config config;

  public TypesafeKafkaesqueConsumerConfig(Config config) {
    this.config = config;
  }

  @Override
  public Properties toProperties() {
    final Properties props = new Properties();

    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("group-id"));
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("auto-offset-reset"));

    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getBoolean("enable-auto-commit"));
    // FIXME What happens if the auto-commit-interval is not set?
    props.put(
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
        Math.toIntExact(config.getDuration("auto-commit-interval").toMillis()));
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getString("client-id"));
    // FIXME What happens if the fetch-max-wait is not set?
    props.put(
        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
        Math.toIntExact(config.getDuration("fetch-max-wait").toMillis()));
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, config.getInt("fetch-min-size"));
    // FIXME What happens if the heartbeat-interval is not set?
    props.put(
        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
        Math.toIntExact(config.getDuration("heartbeat-interval").toMillis()));
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, config.getString("isolation-level"));
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getInt("max-poll-records"));

    return props;
  }
}
