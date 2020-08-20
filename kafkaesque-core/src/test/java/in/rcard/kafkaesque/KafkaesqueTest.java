package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;

import in.rcard.kafkaesque.KafkaesqueConsumer.Builder;
import org.junit.jupiter.api.Test;

// XXX There is a big deal with the code structured this way. We can test only the happy path :(
//     Try to rethink the Kafkaesque type to allow a complete testing strategy.
class KafkaesqueTest {

  @Test
  void usingBrokerShouldReturnAnInstanceOfAConcreteKafkaesque() {
    final Kafkaesque kafkaesque = Kafkaesque.usingBroker("broker");
    assertThat(kafkaesque)
        .isNotNull()
        .isInstanceOf(TestKafkaesque.class);
  }

  private static class TestKafkaesque implements Kafkaesque {
  
    public TestKafkaesque(String broker) {
      // Empty body
    }
  
    @Override
    public <Key, Value> Builder<Key, Value> consume() {
      return null;
    }
  
    @Override
    public <Key, Value> KafkaesqueProducer.Builder<Key, Value> produce() {
      return null;
    }
  }
}