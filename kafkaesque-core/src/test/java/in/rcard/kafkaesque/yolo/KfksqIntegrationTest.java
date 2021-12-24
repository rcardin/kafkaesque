package in.rcard.kafkaesque.yolo;

import static org.assertj.core.api.Assertions.assertThat;

import in.rcard.kafkaesque.Kafkaesque;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class KfksqIntegrationTest {

  @Container
  private final KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

  private String brokerUrl;

  @BeforeEach
  void setUp() {
    brokerUrl = kafka.getBootstrapServers();
  }

  @Test
  void createInputTopicShouldReturnAKafkaesqueInputTopic() {
    final InputTopic<Integer, String> inputTopic =
        Kfksq.at(brokerUrl)
            .createInputTopic("input", new IntegerSerializer(), new StringSerializer());
    assertThat(inputTopic).isNotNull();
  }

  @Test
  void createOutputTopicShouldReturnAKafkaesqueOutputTopic() {
    final OutputTopic<Integer, String> outputTopic =
        Kfksq.at(brokerUrl)
            .createOutputTopic("input", new IntegerDeserializer(), new StringDeserializer());
    assertThat(outputTopic).isNotNull();
  }
}
