package in.rcard.kafkaesque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.contains;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConsumedResultsTest {

  @Mock private ConsumerRecord<String, String> record;

  private List<ConsumerRecord<String, String>> records;
  private ConsumedResults<String, String> consumedResults;

  @BeforeEach
  void setUp() {
    this.records = Collections.singletonList(record);
    this.consumedResults = new ConsumedResults<>(records);
  }

  @Test
  void hasRecordsSizeShouldReturnTheCurrentObjectIfTheGivenSizeIsTheSizeOfTheInternalList() {
    assertThat(consumedResults.hasRecordsSize(1)).isEqualTo(consumedResults);
  }

  @Test
  void hasRecordsSizeShouldThrownAnAssertionErrorIfTheSizeOfTheInternalListIsNotAsExpected() {
    assertThatThrownBy(() -> consumedResults.hasRecordsSize(2))
        .isInstanceOf(AssertionError.class)
        .hasMessage(
            "The desired size of consumed messages 2 is not equal to the effective "
                + "number of read messages 1");
  }

  @Test
  void havingHeadersShouldReturnTheCurrentObjectIfTheAssertionBlockVerifiesCorrectly() {
    final Headers headers = mock(Headers.class);
    final Header correlationIdHeader = mock(Header.class);
    final byte[] correlationIdBytes = "12345".getBytes();
    given(correlationIdHeader.value()).willReturn(correlationIdBytes);
    given(headers.lastHeader("CORRELATION_ID")).willReturn(correlationIdHeader);
    given(record.headers()).willReturn(headers);
    final ConsumedResults<String, String> consumerResultsWithHeaders =
        new ConsumedResults<>(records);
    assertThat(
            consumerResultsWithHeaders.havingHeaders(
                headersList -> {
                  final Optional<Headers> maybeAnHeader =
                      headersList.stream()
                          .filter(
                              hs -> hs.lastHeader("CORRELATION_ID").value() == correlationIdBytes)
                          .findFirst();
                  assertThat(maybeAnHeader).contains(headers);
                }))
        .isEqualTo(consumerResultsWithHeaders);
  }

  @Test
  void havingHeadersShouldThrownAnAssertionErrorIfTheAssertionBlockIsNotSatisfied() {
    assertThatThrownBy(
            () -> consumedResults.havingHeaders(headers -> assertThat(headers).hasSize(2)))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void havingKeysShouldReturnTheCurrentObjectIfTheAssertionBlockVerifiesCorrectly() {
    given(record.key()).willReturn("key");
    final ConsumedResults<String, String> consumedResultsWithAKey = new ConsumedResults<>(records);
    assertThat(
            consumedResultsWithAKey.havingKeys(
                keys -> {
                  final Optional<String> maybeAKey =
                      keys.stream().filter(key -> key.equals("key")).findFirst();
                  assertThat(maybeAKey).contains("key");
                }))
        .isEqualTo(consumedResultsWithAKey);
  }

  @Test
  void havingKeysShouldThrownAnAssertionErrorIfTheAssertionBlockIsNotSatisfied() {
    assertThatThrownBy(() -> consumedResults.havingKeys(keys -> assertThat(keys).hasSize(2)))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void havingPayloadsShouldReturnTheCurrentObjectIfTheAssertionBlockVerifiesCorrectly() {
    given(record.value()).willReturn("42");
    final ConsumedResults<String, String> consumedResultsWithAValue =
        new ConsumedResults<>(records);
    assertThat(
            consumedResultsWithAValue.havingPayloads(
                payloads -> {
                  final Optional<String> maybeAPayload =
                      payloads.stream().filter(payload -> payload.equals("42")).findFirst();
                  assertThat(maybeAPayload).contains("42");
                }))
        .isEqualTo(consumedResultsWithAValue);
  }

  @Test
  void havingPayloadsShouldThrownAnAssertionErrorIfTheAssertionBlockIsNotSatisfied() {
    assertThatThrownBy(
            () -> consumedResults.havingPayloads(payloads -> assertThat(payloads).hasSize(2)))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void havingConsumerRecordsShouldReturnTheCurrentObjectIfTheAssertionBlockVerifiesCorrectly() {
    given(record.value()).willReturn("42");
    final ConsumedResults<String, String> consumedResultsWithAValue =
        new ConsumedResults<>(records);
    assertThat(
            consumedResultsWithAValue.havingConsumerRecords(
                consumerRecords -> {
                  final Optional<ConsumerRecord<String, String>> maybeAConsumerRecord =
                      consumerRecords.stream().filter(cr -> cr.value().equals("42")).findFirst();
                  assertThat(maybeAConsumerRecord).map(ConsumerRecord::value).contains("42");
                }))
        .isEqualTo(consumedResultsWithAValue);
  }

  @Test
  void havingConsumerRecordsShouldThrownAnAssertionErrorIfTheAssertionBlockIsNotSatisfied() {
    assertThatThrownBy(
            () ->
                consumedResults.havingConsumerRecords(
                    consumerRecords -> assertThat(consumerRecords).hasSize(2)))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void assertingThatPayloadsShouldReturnTheCurrentObjectIfTheAssertionBlockVerifiesCorrectly() {
    given(record.value()).willReturn("42");
    final ConsumedResults<String, String> consumedResultsWithAValue =
        new ConsumedResults<>(records);
    assertThat(consumedResultsWithAValue.assertingThatPayloads(contains("42")))
        .isEqualTo(consumedResultsWithAValue);
  }

  @Test
  void assertingThatPayloadsShouldThrownAnAssertionErrorIfTheAssertionBlockIsNotSatisfied() {
    given(record.value()).willReturn("42");
    final ConsumedResults<String, String> consumedResultsWithAValue =
        new ConsumedResults<>(records);
    assertThatThrownBy(() -> consumedResultsWithAValue.assertingThatPayloads(contains("41")))
        .isInstanceOf(AssertionError.class)
        .hasMessage("Expected iterable containing [\"41\"] but item 0: was \"42\"");
  }
}
