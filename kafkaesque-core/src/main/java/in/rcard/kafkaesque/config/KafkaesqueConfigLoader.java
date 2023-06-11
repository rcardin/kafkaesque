package in.rcard.kafkaesque.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;

public class KafkaesqueConfigLoader {

    public static KafkaesqueConsumerConfig loadConsumerConfig() {
        Config config = ConfigFactory.load();
        return new KafkaesqueConsumerConfigImpl(config.getConfig("kafkaesque.consumer"));
    }

    public static KafkaesqueProducerConfig loadProducerConfig() {
        Config config = ConfigFactory.load();
        return new KafkaesqueProducerConfigImpl(config.getConfig("kafkaesque.producer"));
    }
}
