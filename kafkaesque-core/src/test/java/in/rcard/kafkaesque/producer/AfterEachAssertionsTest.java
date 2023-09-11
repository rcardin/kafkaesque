package in.rcard.kafkaesque.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

import in.rcard.kafkaesque.producer.KafkaesqueProducer.Record;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AfterEachAssertionsTest {
  
  @Mock
  private KafkaesqueProducer<String, String> producer;
  
  private final Record<String, String> firstRecord = Record.of("data1", "value1");
  private final List<Record<String, String>> records = List.of(firstRecord);
  
  private final ProducerRecord<String, String> producerRecord =
      new ProducerRecord<>("topic", "key", "value");
  
  private AfterEachAssertions<String, String> afterEachAssertions;

  @BeforeEach
  void setUp() {
    afterEachAssertions = new AfterEachAssertions<>(producer, records, Duration.ofMillis(200));
    given(producer.sendRecord(firstRecord)).willReturn(producerRecord);
  }

  @Test
  void assertingShouldThrowAnAssertionErrorIfConsumingAMessageTakesTooMuch() {
    assertThatThrownBy(
        () ->
            afterEachAssertions.asserting(
                pr -> {
                  try {
                    Thread.sleep(500L);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                }))
        .isInstanceOf(AssertionError.class)
        .hasMessage("The consuming of the message ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null) takes more than 200 milliseconds");
  }
  
  @Test
  void assertingShouldThrowAnAssertionErrorIfTheConsumerAssertionFails() {
    assertThatThrownBy(() -> afterEachAssertions.asserting(pr -> assertThat(pr).isNull()))
        .isInstanceOf(AssertionError.class)
        .hasMessage("\nexpected: null\n" +
                        " but was: ProducerRecord(topic=topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=key, value=value, timestamp=null)");
  }
  
  @Test
  void assertingShouldExecuteWithoutAnyExceptionOnProducerRecord() {
    afterEachAssertions.asserting(pr -> assertThat(pr).isEqualTo(producerRecord));
  }
}