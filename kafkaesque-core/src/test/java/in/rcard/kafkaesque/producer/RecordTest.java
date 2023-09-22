package in.rcard.kafkaesque.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import in.rcard.kafkaesque.producer.KafkaesqueProducer.Header;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

class RecordTest {

  @Test
  void ofWithKeyAndValueShouldCreateAValidRecordWithoutHeaders() {
    final var actualRecord = KafkaesqueProducer.Record.of("key", "value");

    assertThat(actualRecord)
        .extracting("key", "value", "headers")
        .contains("key", "value", Collections.emptyList());
  }

  @Test
  void ofWithKeyAndValueShouldCreateAValidRecordWithHeaders() {
    final Header aHeader = Header.header("hKey", "hValue");
    var actualRecord = KafkaesqueProducer.Record.of("key", "value", aHeader);

    assertThat(actualRecord)
        .extracting("key", "value", "headers")
        .contains("key", "value", List.of(aHeader));
  }

  @Test
  void ofWithKeyAndValueShouldThrownAnExceptionIfTheKeyIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaesqueProducer.Record.of(null, "value"),
        "The key of the record cannot be null");
  }

  @Test
  void ofWithProducerRecordShouldCreateAValidRecordWithoutHeaders() {
    final var producerRecord = new ProducerRecord<>("topic", "key", "value");
    final var actualRecord = KafkaesqueProducer.Record.of(producerRecord);

    assertThat(actualRecord)
        .extracting("key", "value", "headers")
        .contains("key", "value", Collections.emptyList());
  }

  @Test
  void ofWithProducerRecordShouldCreateAValidRecordWithHeaders() {
    final var producerRecord = new ProducerRecord<>("topic", "key", "value");
    producerRecord.headers().add("hKey", "hValue".getBytes());
    final var actualRecord = KafkaesqueProducer.Record.of(producerRecord);

    assertThat(actualRecord)
        .extracting("key", "value", "headers")
        .contains("key", "value", List.of(Header.header("hKey", "hValue".getBytes())));
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
    var record = KafkaesqueProducer.Record.of("key", "value");
    final ProducerRecord<String, String> actualProducerRecord = record.toPr("topic");

    assertThat(actualProducerRecord)
        .extracting("topic", "key", "value", "headers")
        .contains("topic", "key", "value", new RecordHeaders());
  }
}
