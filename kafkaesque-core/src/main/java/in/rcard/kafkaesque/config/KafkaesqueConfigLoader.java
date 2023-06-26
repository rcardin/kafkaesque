package in.rcard.kafkaesque.config;

// TODO Add some documentation
public interface KafkaesqueConfigLoader {
    KafkaesqueConsumerConfig loadConsumerConfig();

    KafkaesqueProducerConfig loadProducerConfig();
}
