package in.rcard.kafkaesque.config;

import java.util.Properties;

/**
 * Represents a Kafka consumer config provider, implementation should provide
 * a mapping between loadable configuration and Kafka's consumer configuration API
 *
 * @see <a href="https://kafka.apache.org/documentation/#consumerconfigs">Apache Kafka Consumer Config API</a>
 */
public interface KafkaesqueConsumerConfig {

  Properties toProperties();
}
