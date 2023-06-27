package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TypesafeKafkaesqueConfigLoader implements KafkaesqueConfigLoader {

    @Override
    public KafkaesqueConsumerConfig loadConsumerConfig() {
        // TODO Add the path to the configuration file as a constructor parameter
        Config config = ConfigFactory.load();
        return new TypesafeKafkaesqueConsumerConfig(config.getConfig("kafkaesque.consumer"));
    }

    @Override
    public KafkaesqueProducerConfig loadProducerConfig() {
        // TODO Add the path to the configuration file as a constructor parameter
        Config config = ConfigFactory.load();
        return new TypesafeKafkaesqueProducerConfig(config.getConfig("kafkaesque.producer"));
    }
}
