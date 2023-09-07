package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypesafeKafkaesqueProducerConfig implements KafkaesqueProducerConfig {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TypesafeKafkaesqueProducerConfig.class);

  private final Config config;

  /**
   * Represents a Kafka producer config provider, implementation should provide a mapping between
   * loadable configuration and Kafka's producer configuration API
   *
   * @see <a href="https://kafka.apache.org/documentation/#producerconfigs">Apache Kafka Producer
   *     Config API</a>
   */
  public TypesafeKafkaesqueProducerConfig(Config config) {
    this.config = config;
  }

  @Override
  public Properties toProperties() {
    final Properties props = new Properties();

    configureIfPresent(props, ProducerConfig.CLIENT_ID_CONFIG, () -> config.getString("client-id"));
    configureIfPresent(props, ProducerConfig.RETRIES_CONFIG, () -> config.getInt("retries"));
    configureIfPresent(props, ProducerConfig.ACKS_CONFIG, () -> config.getString("acks"));
    configureIfPresent(props, ProducerConfig.BATCH_SIZE_CONFIG, () -> config.getInt("batch-size"));
    configureIfPresent(
        props, ProducerConfig.BUFFER_MEMORY_CONFIG, () -> config.getLong("buffer-memory"));
    configureIfPresent(
        props, ProducerConfig.COMPRESSION_TYPE_CONFIG, () -> config.getString("compression-type"));

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
