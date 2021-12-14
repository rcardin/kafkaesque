package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import in.rcard.kafkaesque.producer.KafkaesqueProducerDSL;
import org.junit.jupiter.api.Test;

class KafkaesqueTest {

  @Test
  void atShouldReturnAnInstanceOfAConcreteKafkaesque() {
    final Kafkaesque kafkaesque = Kafkaesque.at("broker");
    assertThat(kafkaesque).isNotNull();
  }

  @Test
  void atShouldThrowAnIllegalArgumentExceptionIfTheBrokerUrlIsNull() {
    assertThatThrownBy(() -> Kafkaesque.at(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Brokers URL cannot be empty");
  }
  
  @Test
  void atShouldThrowAnIllegalArgumentExceptionIfTheBrokerUrlIsEmpty() {
    assertThatThrownBy(() -> Kafkaesque.at(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Brokers URL cannot be empty");
  }
  
  @Test
  void consumeShouldReturnAnInstanceOfAKafkaesqueConsumerBuilder() {
    final Kafkaesque kafkaesque = Kafkaesque.at("broker");
    final KafkaesqueConsumer.Builder<String, String> consumer = kafkaesque.consume();
    assertThat(consumer).isNotNull();
  }

  @Test
  void produceShouldReturnAnInstanceOfAKafkaesqueProducerBuilder() {
    final Kafkaesque kafkaesque = Kafkaesque.at("broker");
    final KafkaesqueProducerDSL<String, String> producer = kafkaesque.produce();
    assertThat(producer).isNotNull();
  }
}
