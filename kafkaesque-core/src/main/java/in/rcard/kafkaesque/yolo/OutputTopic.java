package in.rcard.kafkaesque.yolo;

import static in.rcard.kafkaesque.common.Header.header;

import in.rcard.kafkaesque.common.Header;
import in.rcard.kafkaesque.consumer.KafkaesqueConsumerDSL;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

public class OutputTopic<Key, Value> {

  private final KafkaesqueConsumerDSL<Key, Value> dsl;

  OutputTopic(KafkaesqueConsumerDSL<Key, Value> dsl) {
    this.dsl = dsl;
  }

  public List<Message<Key, Value>> readRecordsToList() {
    final List<Message<Key, Value>> messages = new ArrayList<>();
    dsl.expectingConsumed()
        .havingConsumerRecords(
            consumerRecords -> consumerRecords.forEach(record -> messages.add(Message.of(record))))
        .andCloseConsumer();
    return messages;
  }

  public static class Message<Key, Value> {
    private final Key key;
    private final Value value;

    private final List<Header> headers;

    private Message(Key key, Value value, List<Header> headers) {
      this.key = key;
      this.value = value;
      this.headers = headers;
    }

    static <Key, Value> Message<Key, Value> of(ConsumerRecord<Key, Value> record) {
      return new Message<>(record.key(), record.value(), adaptKafkaHeaders(record.headers()));
    }

    private static ArrayList<Header> adaptKafkaHeaders(Headers kafkaHeaders) {
      final ArrayList<Header> headersList = new ArrayList<>();
      kafkaHeaders.forEach(header -> headersList.add(header(header.key(), header.value())));
      return headersList;
    }

    public Key getKey() {
      return key;
    }

    public Value getValue() {
      return value;
    }

    public List<Header> getHeaders() {
      return headers;
    }

    public List<Header> headers(String key) {
      return headers.stream()
          .filter(header -> header.getKey().equals(key))
          .collect(Collectors.toList());
    }

    public Optional<Header> lastHeader(String key) {
      return headers.stream()
          .filter(header -> header.getKey().equals(key))
          .reduce((first, second) -> second);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Message<?, ?> message = (Message<?, ?>) o;
      return Objects.equals(key, message.key) && Objects.equals(value, message.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }

    @Override
    public String toString() {
      return "Message{" + "key=" + key + ", value=" + value + '}';
    }
  }
}
