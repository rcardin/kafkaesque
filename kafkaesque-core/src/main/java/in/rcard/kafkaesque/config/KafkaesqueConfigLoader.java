package in.rcard.kafkaesque.config;

/**
 * Kafka consumer and producer config loading contract, implementation requires
 * the definition of concrete {@code KafkaesqueConsumerConfig} and {@code KafkaesqueProducerConfig}
 *
 * @see KafkaesqueConsumerConfig
 * @see KafkaesqueProducerConfig
 */
public interface KafkaesqueConfigLoader {
    KafkaesqueConsumerConfig loadConsumerConfig();

    KafkaesqueProducerConfig loadProducerConfig();
}
