package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TypesafeKafkaesqueConfigLoader implements KafkaesqueConfigLoader {

    @Override
    public KafkaesqueConsumerConfig loadConsumerConfig() {
        Config config = ConfigFactory.load();
        return new KafkaesqueConsumerConfigImpl(config.getConfig("kafkaesque.consumer"));
    }

    @Override
    public KafkaesqueProducerConfig loadProducerConfig() {
        Config config = ConfigFactory.load();
        return new KafkaesqueProducerConfigImpl(config.getConfig("kafkaesque.producer"));
    }
}
