package in.rcard.kafkaesque.yolo;

import static org.assertj.core.api.Assertions.assertThat;

import in.rcard.kafkaesque.Kafkaesque;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class KfksqTest {
//  @Test
//  void createInputTopicShouldReturnAKafkaesqueInputTopic() {
//    final InputTopic<Integer, String> inputTopic =
//        Kafkaesque.at("broker")
//            .createInputTopic("input", new IntegerSerializer(), new StringSerializer());
//    assertThat(inputTopic).isNotNull();
//  }
//
//  @Test
//  void createOutputTopicShouldReturnAKafkaesqueOutputTopic() {
//    final OutputTopic<Integer, String> outputTopic =
//        Kafkaesque.at("broker")
//            .createOutputTopic("input", new IntegerDeserializer(), new StringDeserializer());
//    assertThat(outputTopic).isNotNull();
//  }
}