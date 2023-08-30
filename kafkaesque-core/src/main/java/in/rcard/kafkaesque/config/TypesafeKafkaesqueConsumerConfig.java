package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypesafeKafkaesqueConsumerConfig implements KafkaesqueConsumerConfig {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TypesafeKafkaesqueConsumerConfig.class);

  private final Config config;

  public TypesafeKafkaesqueConsumerConfig(Config config) {
    this.config = config;
  }

  @Override
  public Properties toProperties() {
    final Properties props = new Properties();

    configureIfPresent(props, ConsumerConfig.GROUP_ID_CONFIG, () -> config.getString("group-id"));
    configureIfPresent(
        props,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        () -> config.getString("auto-offset-reset"));

    configureIfPresent(
        props,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        () -> config.getBoolean("enable-auto-commit"));
    // FIXME What happens if the auto-commit-interval is not set?
    configureIfPresent(
        props,
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
        () -> Math.toIntExact(config.getDuration("auto-commit-interval").toMillis()));
    configureIfPresent(props, ConsumerConfig.CLIENT_ID_CONFIG, () -> config.getString("client-id"));
    // FIXME What happens if the fetch-max-wait is not set?
    configureIfPresent(
        props,
        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
        () -> Math.toIntExact(config.getDuration("fetch-max-wait").toMillis()));
    configureIfPresent(
        props, ConsumerConfig.FETCH_MIN_BYTES_CONFIG, () -> config.getInt("fetch-min-size"));
    // FIXME What happens if the heartbeat-interval is not set?
    configureIfPresent(
        props,
        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
        () -> Math.toIntExact(config.getDuration("heartbeat-interval").toMillis()));
    configureIfPresent(
        props, ConsumerConfig.ISOLATION_LEVEL_CONFIG, () -> config.getString("isolation-level"));
    configureIfPresent(
        props, ConsumerConfig.MAX_POLL_RECORDS_CONFIG, () -> config.getInt("max-poll-records"));

    return props;
  }

  private <T> void configureIfPresent(
      Properties props, String propertyName, Supplier<T> configValueSupplier) {
    try {
      final T configValue = configValueSupplier.get();
      props.put(propertyName, configValue);
    } catch (ConfigException e) {
      LOGGER.debug("Value for property '{}' is missing. Skipping it", propertyName);
    }
  }
}
