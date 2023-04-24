package in.rcard.kafkaesque.consumer;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaesqueConsumerConfigMapper {

    public static Map toConsumerProperties(KafkaesqueConsumerConfig kConsumerConfig) {
        final Map props = new Properties();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, kConsumerConfig.groupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kConsumerConfig.autoOffsetReset());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kConsumerConfig.bootstrapServers());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kConsumerConfig.enableAutoCommit());
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Math.toIntExact(kConsumerConfig.autoCommitInterval().toMillis()));
        props.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kConsumerConfig.keyDeserializer());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kConsumerConfig.valueDeserializer());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, kConsumerConfig.clientId());
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Math.toIntExact(kConsumerConfig.fetchMaxWait().toMillis()));
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, kConsumerConfig.fetchMinSize());
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, Math.toIntExact(kConsumerConfig.heartbeatInterval().toMillis()));
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, kConsumerConfig.isolationLevel());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kConsumerConfig.maxPollRecords());

        return props;
    }
}
