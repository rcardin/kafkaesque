package in.rcard.kafkaesque.producer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

import in.rcard.kafkaesque.producer.KafkaesqueProducer.Record;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AfterAllAssertionsTest {
  
  @Mock
  private KafkaesqueProducer<String, String> producer;
  
  private final List<Record<String, String>> records = List.of(
      Record.of("data1", "value1"),
      Record.of("data2", "value2")
  );
  
  private AfterAllAssertions<String, String> afterAllAssertions;
  
  @BeforeEach
  void setUp() {
    afterAllAssertions = new AfterAllAssertions<>(
        producer,
        records,
        Duration.ofMillis(200)
    );
  }

  @Test
  void assertingShouldThrowAnAssertionErrorIfConsumingTheMessagesTakesTooMuch() {
    given(producer.sendRecords(records)).willReturn(Collections.emptyList());
    assertThatThrownBy(() -> afterAllAssertions.asserting(producerRecords -> {
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }))
        .isInstanceOf(AssertionError.class)
        .hasMessage("Consuming the list of [] messages takes more than 200 milliseconds");
  }
}