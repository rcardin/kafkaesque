package in.rcard.kafkaesque.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

import in.rcard.kafkaesque.common.Header;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class RecordTest {

  private static final String A_KEY = "key";
  private static final String A_VALUE = "value";
  public static final String A_HEADER_KEY = "hKey";
  private static final String A_HEADER_VALUE = "hValue";
  private static final String A_TOPIC = "topic";

  @Test
  void ofWithKeyAndValueShouldCreateAValidRecordWithoutHeaders() {
    final var actualRecord = KafkaesqueProducer.Record.of(A_KEY, A_VALUE);

    assertThat(actualRecord)
        .extracting("key", "value", "headers")
        .contains(A_KEY, A_VALUE, Collections.emptyList());
  }

  @Test
  void ofWithKeyAndValueShouldCreateAValidRecordWithHeaders() {
    final Header aHeader = Header.header(A_HEADER_KEY, A_HEADER_VALUE);
    var actualRecord = KafkaesqueProducer.Record.of(A_KEY, A_VALUE, aHeader);

    assertThat(actualRecord)
        .extracting("key", "value", "headers")
        .contains(A_KEY, A_VALUE, List.of(aHeader));
  }

  @Test
  void ofWithKeyAndValueShouldThrownAnExceptionIfTheKeyIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaesqueProducer.Record.of(null, A_VALUE),
        "The key of the record cannot be null");
  }

  @Test
  void ofWithProducerRecordShouldCreateAValidRecordWithoutHeaders() {
    final var producerRecord = new ProducerRecord<>(A_TOPIC, A_KEY, A_VALUE);
    final var actualRecord = KafkaesqueProducer.Record.of(producerRecord);

    assertThat(actualRecord)
        .extracting("key", "value", "headers")
        .contains(A_KEY, A_VALUE, Collections.emptyList());
  }

  @Test
  void ofWithProducerRecordShouldCreateAValidRecordWithHeaders() {
    final var producerRecord = new ProducerRecord<>(A_TOPIC, A_KEY, A_VALUE);
    producerRecord.headers().add(A_HEADER_KEY, A_HEADER_VALUE.getBytes());
    final var actualRecord = KafkaesqueProducer.Record.of(producerRecord);

    assertThat(actualRecord)
        .extracting("key", "value", "headers")
        .contains(A_KEY, A_VALUE, List.of(Header.header(A_HEADER_KEY, A_HEADER_VALUE.getBytes())));
  }

  @Test
  void ofWithProducerRecordShouldThrownAnExceptionIfTheProducerRecordIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaesqueProducer.Record.of((ProducerRecord<String, String>) null),
        "The producer record cannot be null");
  }

  @Test
  void toPrShouldCreateAValidProducerRecordWithoutHeaders() {
    var record = KafkaesqueProducer.Record.of(A_KEY, A_VALUE);
    final ProducerRecord<String, String> actualProducerRecord = record.toPr(A_TOPIC);

    assertThat(actualProducerRecord)
        .extracting("topic", "key", "value", "headers")
        .contains(A_TOPIC, A_KEY, A_VALUE, new RecordHeaders());
  }

  @Test
  void toPrShouldCreateAValidProduceRecordWithHeaders() {
    var record =
        KafkaesqueProducer.Record.of(A_KEY, A_VALUE, Header.header(A_HEADER_KEY, A_HEADER_VALUE));
    final ProducerRecord<String, String> actualProducerRecord = record.toPr(A_TOPIC);

    final Headers expectedHeaders = new RecordHeaders();
    expectedHeaders.add(new RecordHeader(A_HEADER_KEY, A_HEADER_VALUE.getBytes()));

    assertThat(actualProducerRecord)
        .extracting("topic", "key", "value", "headers")
        .contains(A_TOPIC, A_VALUE, A_VALUE, expectedHeaders);
  }

  @Test
  void toPrShouldThrownAnExceptionIfTheGivenTopicIsNull() {
    var record = KafkaesqueProducer.Record.of(A_KEY, A_VALUE);
    assertThatThrownBy(() -> record.toPr(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic cannot be null or empty");
  }

  @Test
  void toPrShouldThrownAnExceptionIfTheGivenTopicIsEmpty() {
    var record = KafkaesqueProducer.Record.of(A_KEY, A_VALUE);
    assertThatThrownBy(() -> record.toPr(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The topic cannot be null or empty");
  }
}
