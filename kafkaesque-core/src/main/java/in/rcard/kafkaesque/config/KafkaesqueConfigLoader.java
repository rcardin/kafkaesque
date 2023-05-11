package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;

public class KafkaesqueConfigLoader {

    public static KafkaesqueConsumerConfig loadConsumerConfig() {
        Config config = ConfigFactory.load();
        return ConfigBeanFactory.create(config.getConfig("kafkaesque.consumer"), KafkaesqueConsumerConfigImpl.class);
    }

    public static KafkaesqueProducerConfig loadProducerConfig() {
        Config config = ConfigFactory.load();
        return ConfigBeanFactory.create(config.getConfig("kafkaesque.producer"), KafkaesqueProducerConfigImpl.class);
    }
}
