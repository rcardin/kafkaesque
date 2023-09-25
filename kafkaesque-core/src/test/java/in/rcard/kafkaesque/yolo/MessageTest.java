package in.rcard.kafkaesque.yolo;

import static in.rcard.kafkaesque.common.Header.header;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

class MessageTest {

  private static final String A_TOPIC = "topic";
  private static final String A_KEY = "key";
  private static final String A_VALUE = "value";
  private static final String A_HEADER_KEY = "hKey";
  private static final byte[] A_HEADER_VALUE = "hValue".getBytes();
  public static final byte[] ANOTHER_HEADER_VALUE = "anotherValue".getBytes();

  @Test
  void ofShouldCreateAMessageWithoutHeaders() {
    final var consumerRecord = new ConsumerRecord<>(A_TOPIC, 1, 0L, A_KEY, A_VALUE);
    final var actualMessage = OutputTopic.Message.of(consumerRecord);

    assertThat(actualMessage)
        .extracting("key", "value", "headers")
        .containsExactly(A_KEY, A_VALUE, Collections.emptyList());
  }

  @Test
  void ofShouldCreateAMessageWithHeaders() {
    final var consumerRecord = new ConsumerRecord<>(A_TOPIC, 1, 0L, A_KEY, A_VALUE);
    consumerRecord.headers().add(new RecordHeader(A_HEADER_KEY, A_HEADER_VALUE));
    final var actualMessage = OutputTopic.Message.of(consumerRecord);

    assertThat(actualMessage)
        .extracting("key", "value", "headers")
        .containsExactly(A_KEY, A_VALUE, List.of(header(A_HEADER_KEY, A_HEADER_VALUE)));
  }

  @Test
  void headersByKeyShouldReturnHeadersWithTheSameKey() {
    final var consumerRecord = new ConsumerRecord<>(A_TOPIC, 1, 0L, A_KEY, A_VALUE);
    consumerRecord.headers().add(new RecordHeader(A_HEADER_KEY, A_HEADER_VALUE));
    consumerRecord.headers().add(new RecordHeader(A_HEADER_KEY, A_HEADER_VALUE));
    consumerRecord.headers().add(new RecordHeader("anotherKey", A_HEADER_VALUE));
    final var actualMessage = OutputTopic.Message.of(consumerRecord);

    assertThat(actualMessage.headers(A_HEADER_KEY))
        .containsExactly(
            header(A_HEADER_KEY, A_HEADER_VALUE), header(A_HEADER_KEY, A_HEADER_VALUE));
  }

  @Test
  void headersByKeyShouldReturnTheEmptyListIfNoHeadersWithTheSameKey() {
    final var consumerRecord = new ConsumerRecord<>(A_TOPIC, 1, 0L, A_KEY, A_VALUE);
    consumerRecord.headers().add(new RecordHeader("anotherKey", A_HEADER_VALUE));
    final var actualMessage = OutputTopic.Message.of(consumerRecord);

    assertThat(actualMessage.headers(A_HEADER_KEY)).isEmpty();
  }

  @Test
  void headersByKeyShouldReturnTheEmptyListIfNoHeadersAtAll() {
    final var consumerRecord = new ConsumerRecord<>(A_TOPIC, 1, 0L, A_KEY, A_VALUE);
    final var actualMessage = OutputTopic.Message.of(consumerRecord);

    assertThat(actualMessage.headers(A_HEADER_KEY)).isEmpty();
  }

  @Test
  void lastHeaderShouldReturnTheLastHeaderWithAKey() {
    final var consumerRecord = new ConsumerRecord<>(A_TOPIC, 1, 0L, A_KEY, A_VALUE);
    consumerRecord.headers().add(new RecordHeader(A_HEADER_KEY, A_HEADER_VALUE));
    consumerRecord.headers().add(new RecordHeader(A_HEADER_KEY, ANOTHER_HEADER_VALUE));
    final var actualMessage = OutputTopic.Message.of(consumerRecord);

    assertThat(actualMessage.lastHeader(A_HEADER_KEY))
        .contains(header(A_HEADER_KEY, ANOTHER_HEADER_VALUE));
  }

  @Test
  void lastHeaderShouldReturnTheEmptyOptionalIfNoHeaderWithTheGivenKey() {
    final var consumerRecord = new ConsumerRecord<>(A_TOPIC, 1, 0L, A_KEY, A_VALUE);
    consumerRecord.headers().add(new RecordHeader("anotherKey", A_HEADER_VALUE));
    final var actualMessage = OutputTopic.Message.of(consumerRecord);

    assertThat(actualMessage.lastHeader(A_HEADER_KEY)).isEmpty();
  }

  @Test
  void lastHeaderShouldReturnTheEmptyOptionalIfNoHeaderAtAll() {
    final var consumerRecord = new ConsumerRecord<>(A_TOPIC, 1, 0L, A_KEY, A_VALUE);
    final var actualMessage = OutputTopic.Message.of(consumerRecord);

    assertThat(actualMessage.lastHeader(A_HEADER_KEY)).isEmpty();
  }
}
