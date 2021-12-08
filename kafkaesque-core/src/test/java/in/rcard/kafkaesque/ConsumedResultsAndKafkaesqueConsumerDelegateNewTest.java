package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class})
class ConsumedResultsAndKafkaesqueConsumerDelegateNewTest {

  @Mock private KafkaesqueConsumer<String, String> consumer;
  @Mock private ConsumedResults<String, String> results;
  @InjectMocks private ConsumedResultsAndKafkaesqueConsumerDelegate<String, String> delegate;

  @Test
  void andCloseConsumerShouldCallTheSameMethodOnTheConsumer() {
    delegate.andCloseConsumer();
    verify(consumer, times(1)).andCloseConsumer();
  }

  @Test
  void havingRecordsSizeShouldCallTheSameMethodOnTheResults() {
    delegate.havingRecordsSize(42L);
    verify(results, times(1)).havingRecordsSize(42L);
  }

  @Test
  void havingHeadersShouldCallTheSameMethodOnTheResults() {
    final Consumer<List<Headers>> assertion = list -> assertThat(true).isTrue();
    delegate.havingHeaders(assertion);
    verify(results, times(1)).havingHeaders(assertion);
  }

  @Test
  void havingKeysShouldCallTheSameMethodOnTheResults() {
    final Consumer<List<String>> assertion = list -> assertThat(true).isTrue();
    delegate.havingKeys(assertion);
    verify(results, times(1)).havingKeys(assertion);
  }

  @Test
  void havingPayloadsShouldCallTheSameMethodOnTheResults() {
    final Consumer<List<String>> assertion = list -> assertThat(true).isTrue();
    delegate.havingPayloads(assertion);
    verify(results, times(1)).havingPayloads(assertion);
  }

  @Test
  void havingConsumerRecordsShouldCallTheSameMethodOnTheResults() {
    final Consumer<List<ConsumerRecord<String, String>>> assertion =
        list -> assertThat(true).isTrue();
    delegate.havingConsumerRecords(assertion);
    verify(results, times(1)).havingConsumerRecords(assertion);
  }

  @Test
  void assertingThatPayloadsShouldCallTheSameMethodOnTheResults() {
    final Matcher<Collection<? extends String>> matcher = Matchers.empty();
    delegate.assertingThatPayloads(matcher);
    verify(results, times(1)).assertingThatPayloads(matcher);
  }
}
