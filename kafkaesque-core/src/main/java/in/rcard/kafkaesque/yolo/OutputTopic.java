package in.rcard.kafkaesque.yolo;

import in.rcard.kafkaesque.KafkaesqueConsumer.Builder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class OutputTopic<Key, Value> {

  private final Builder<Key, Value> consumerBuilder;
  
  OutputTopic(
      Builder<Key, Value> consumerBuilder) {
    this.consumerBuilder = consumerBuilder;
  }
  
  public List<Message<Key, Value>> readRecordsToList() {
    final List<Message<Key, Value>> messages = new ArrayList<>();
    consumerBuilder
        .expecting()
        .havingConsumerRecords(
            consumerRecords -> consumerRecords.forEach(record -> messages.add(Message.of(record))))
        .andCloseConsumer();
    return messages;
  }
  
  public static class Message<Key, Value> {
      private final Key key;
      private final Value value;
  
    private Message(Key key, Value value) {
      this.key = key;
      this.value = value;
    }
    
    static <Key, Value> Message<Key, Value> of(ConsumerRecord<Key, Value> record) {
      return new Message<>(
          record.key(),
          record.value()
      );
    }
  
    public Key getKey() {
      return key;
    }
  
    public Value getValue() {
      return value;
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
      return Objects.equals(key, message.key) &&
                 Objects.equals(value, message.value);
    }
  
    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }
  
    @Override
    public String toString() {
      return "Message{" +
                 "key=" + key +
                 ", value=" + value +
                 '}';
    }
  }
  
}
