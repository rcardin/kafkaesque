package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class TypesafeKafkaesqueConfigLoader implements KafkaesqueConfigLoader {

  private final Config configuration;

  public TypesafeKafkaesqueConfigLoader(String configurationFilePath) {
    this.configuration = readConfigurationFile(configurationFilePath);
  }

  @Override
  public KafkaesqueConsumerConfig loadConsumerConfig() {
    return new TypesafeKafkaesqueConsumerConfig(configuration.getConfig("kafkaesque.consumer"));
  }

  @Override
  public KafkaesqueProducerConfig loadProducerConfig() {
    return new TypesafeKafkaesqueProducerConfig(configuration.getConfig("kafkaesque.producer"));
  }

  private Config readConfigurationFile(String configurationFilePath) {
    final URL resource = getClass().getClassLoader().getResource(configurationFilePath);
    if (resource == null) {
      throw new IllegalArgumentException(
          String.format("Configuration file '%s' does not exist", configurationFilePath));
    }
    try {
      return ConfigFactory.parseFile(Paths.get(resource.toURI()).toFile());
    } catch (URISyntaxException e) {
      // Can't happen
    }
    return null;
  }
}
