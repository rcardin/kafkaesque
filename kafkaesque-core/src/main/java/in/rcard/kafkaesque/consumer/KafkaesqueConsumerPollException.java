package in.rcard.kafkaesque.consumer;

/**
 * Represents an exception thrown during the poll phase of a consumer
 */
public class KafkaesqueConsumerPollException extends RuntimeException {
  
  public KafkaesqueConsumerPollException(String message, Throwable cause) {
    super(message, cause);
  }
}
