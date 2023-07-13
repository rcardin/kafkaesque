package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;

public class TypesafeKafkaesqueProducerConfig implements KafkaesqueProducerConfig {
  private final Config config;

  public TypesafeKafkaesqueProducerConfig(Config config) {
    this.config = config;
  }

  @Override
  public Properties toProperties() {
    final Properties props = new Properties();

    props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getString("client-id"));
    props.put(ProducerConfig.RETRIES_CONFIG, config.getInt("retries"));
    props.put(ProducerConfig.ACKS_CONFIG, config.getString("acks"));
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getInt("batch-size"));
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getLong("buffer-memory"));
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getString("compression-type"));

    return props;
  }
}
