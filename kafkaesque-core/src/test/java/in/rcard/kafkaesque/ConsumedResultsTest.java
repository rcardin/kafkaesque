package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.mock;

import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConsumedResultsTest {
  
  @Mock
  private ConsumerRecord<String, String> record;
  private List<ConsumerRecord<String, String>> records;
  
  @BeforeEach
  void setUp() {
    this.records = Collections.singletonList(record);
  }
  
  @Test
  void recordsSizeShouldReturnTheCurrentObjectIfTheGivenSizeIsTheSizeOfTheInternalList() {
    final ConsumedResults<String, String> consumedResults = new ConsumedResults<>(records);
    assertThat(consumedResults.recordsSize(1)).isEqualTo(consumedResults);
  }

  @Test
  void recordsSizeShouldThrownAnAssertionErrorIfTheSizeOfTheInternalListIsNotAsExpected() {
    final ConsumedResults<String, String> consumedResults = new ConsumedResults<>(records);
    assertThatThrownBy(() -> consumedResults.recordsSize(2))
        .isInstanceOf(AssertionError.class)
        .hasMessage("The desired size of consumed messages 2 is not equal to the effective "
                        + "number of read messages 1");
  }
}