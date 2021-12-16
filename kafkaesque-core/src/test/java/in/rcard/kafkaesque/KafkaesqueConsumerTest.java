package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

import in.rcard.kafkaesque.consumer.KafkaesqueConsumer;
import in.rcard.kafkaesque.consumer.KafkaesqueConsumerPollException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaesqueConsumerTest {

  @Mock private KafkaesqueConsumerDelegate<String, String> delegate;

  @Mock private ConsumerRecord<String, String> record;

  private KafkaesqueConsumer<String, String> consumer;

  @BeforeEach
  void setUp() {
    consumer =
        new KafkaesqueConsumer<>(
            500L, TimeUnit.MILLISECONDS, 2, 50L, TimeUnit.MILLISECONDS, delegate);
  }

  @Test
  void pollShouldThrowAKafkaesqueConsumerPollExceptionIfSomethingWentWrongDuringThePolling() {
    given(delegate.poll()).willThrow(new RuntimeException("Exception!"));
    assertThatThrownBy(() -> consumer.poll())
        .isInstanceOf(KafkaesqueConsumerPollException.class)
        .hasMessage("Error during the poll operation")
        .hasCauseInstanceOf(RuntimeException.class)
        .getCause()
        .hasMessage("Exception!");
  }

  @Test
  void pollShouldThrowAnAssertionErrorIfTheConditionsOnTheEmptyPollAreNotMet() {
    final List<ConsumerRecord<String, String>> records = Collections.singletonList(record);
    //noinspection unchecked
    given(delegate.poll()).willReturn(records, records, records);
    consumer =
        new KafkaesqueConsumer<>(
            100L, TimeUnit.MILLISECONDS, 2, 50L, TimeUnit.MILLISECONDS, delegate);
    assertThatThrownBy(() -> consumer.poll())
        .isInstanceOf(AssertionError.class)
        .hasMessage(
            "The consumer reads new messages until the end of the given time interval: "
                + "100 MILLISECONDS");
  }

  @Test
  void pollShouldReturnTheListOfConsumerRecords() {
    final List<ConsumerRecord<String, String>> records = Collections.singletonList(record);
    //noinspection unchecked
    given(delegate.poll()).willReturn(records, records, Collections.emptyList());
    assertThat(consumer.poll()).isNotNull().satisfies(cr -> cr.havingRecordsSize(2));
  }

  @Test
  void andCloseConsumerShouldCallTheCloseMethodOfTheDelegateObject() {
    final KafkaesqueConsumer<String, String> consumer =
        new KafkaesqueConsumer<>(
            200L, TimeUnit.MILLISECONDS, 2, 50L, TimeUnit.MILLISECONDS, delegate);
    consumer.andCloseConsumer();
    Mockito.verify(delegate, Mockito.times(1)).close();
  }
}
