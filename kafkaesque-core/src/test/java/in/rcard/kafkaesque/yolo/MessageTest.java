package in.rcard.kafkaesque.yolo;

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
        .containsExactly(
            A_KEY, A_VALUE, List.of(new OutputTopic.Header(A_HEADER_KEY, A_HEADER_VALUE)));
  }
}
