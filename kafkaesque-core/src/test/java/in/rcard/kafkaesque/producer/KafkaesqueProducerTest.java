package in.rcard.kafkaesque.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

import in.rcard.kafkaesque.producer.KafkaesqueProducer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaesqueProducerTest {

//  @Mock private ProducerRecord<String, String> record;
//  private final RecordMetadata metadata =
//      new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 42, 42);
//
//  private KafkaesqueProducer<String, String> producer;
//
//  @BeforeEach
//  void setUp() {
//    producer =
//        new KafkaesqueProducer<>(
//            "broker",
//            Arrays.asList(record, record),
//            Duration.of(200L, ChronoUnit.MILLIS),
//            Duration.of(500L, ChronoUnit.MILLIS));
//  }
//
//  @Test
//  void assertingAfterEachShouldExecuteAssertionAfterEachRecord() {
//    given(producerDelegate.sendRecord(record))
//        .willReturn(CompletableFuture.completedFuture(metadata));
//    final AtomicInteger counter = new AtomicInteger(0);
//    producer.assertingAfterEach(
//        pr -> {
//          assertThat(pr).isEqualTo(record);
//          counter.incrementAndGet();
//        });
//    assertThat(counter).hasValue(2);
//  }
//
//  @Test
//  void assertingAfterEachShouldThrowAnAEIfTheTimeOfProcessingASingleRecordExceedsTheLimit() {
//    final Executor delayedExecutor = CompletableFuture.delayedExecutor(500L, TimeUnit.MILLISECONDS);
//    final CompletableFuture<RecordMetadata> promise = new CompletableFuture<>();
//    given(producerDelegate.sendRecord(record))
//        .willReturn(promise.completeAsync(() -> metadata, delayedExecutor));
//    given(record.toString()).willReturn("ProducerRecord");
//    assertThatThrownBy(() -> producer.assertingAfterEach(pr -> assertThat(pr).isEqualTo(record)))
//        .isInstanceOf(AssertionError.class)
//        .hasMessage("Impossible to send a the record ProducerRecord in 200 milliseconds")
//        .hasCauseInstanceOf(TimeoutException.class);
//  }
//
//  @Test
//  void assertingAfterEachShouldThrowAnAEIfTheConsumingOfAMessageTakesTooLong() {
//    given(producerDelegate.sendRecord(record))
//        .willReturn(CompletableFuture.completedFuture(metadata));
//    given(record.toString()).willReturn("ProducerRecord");
//    assertThatThrownBy(() -> producer.assertingAfterEach(pr -> waitOneSecond()))
//        .isInstanceOf(AssertionError.class)
//        .hasMessage("The consuming of the message ProducerRecord takes more than 500 milliseconds");
//  }
//
//  @Test
//  void assertingAfterAllShouldExecuteAssertionAfterAllTheRecordsAreSent() {
//    given(producerDelegate.sendRecord(record))
//        .willReturn(CompletableFuture.completedFuture(metadata));
//    final AtomicInteger counter = new AtomicInteger(0);
//    producer.assertingAfterAll(
//        records -> {
//          assertThat(records).containsExactly(record, record);
//          counter.incrementAndGet();
//        });
//    assertThat(counter).hasValue(1);
//  }
//
//  @Test
//  void assertingAfterAllShouldThrowAnAEIfTheTimeOfProcessingAllOfTheRecordsExceedsTheLimit() {
//    final Executor delayedExecutor = CompletableFuture.delayedExecutor(500L, TimeUnit.MILLISECONDS);
//    final CompletableFuture<RecordMetadata> promise = new CompletableFuture<>();
//    given(producerDelegate.sendRecord(record))
//        .willReturn(promise.completeAsync(() -> metadata, delayedExecutor));
//    given(record.toString()).willReturn("ProducerRecord");
//    assertThatThrownBy(() -> producer.assertingAfterAll(prs -> assertThat(prs).hasSize(2)))
//        .isInstanceOf(AssertionError.class)
//        .hasMessage("At least the sending of one record of the list "
//                        + "[ProducerRecord, ProducerRecord] takes more than 200 milliseconds")
//        .hasCauseInstanceOf(TimeoutException.class);
//  }
//
//  @Test
//  void assertingAfterAllShouldThrowAnAEIfTheConsumingOfAMessageTakesTooLong() {
//    given(producerDelegate.sendRecord(record))
//        .willReturn(CompletableFuture.completedFuture(metadata));
//    given(record.toString()).willReturn("ProducerRecord");
//    assertThatThrownBy(() -> producer.assertingAfterAll(pr -> waitOneSecond()))
//        .isInstanceOf(AssertionError.class)
//        .hasMessage("The consuming of the list of messages [ProducerRecord, ProducerRecord] "
//                        + "takes more than 500 milliseconds");
//  }
//
//  private void waitOneSecond() {
//    try {
//      Thread.sleep(1000L);
//    } catch (InterruptedException ignored) {
//    }
//  }
}
