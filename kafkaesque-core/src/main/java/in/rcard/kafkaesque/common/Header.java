package in.rcard.kafkaesque.common;

import java.util.Arrays;
import java.util.Objects;
import org.apache.kafka.common.header.internals.RecordHeader;

/** A header to add to a message sent to a Kafka topic. */
public class Header {
  private final String key;
  private final byte[] value;

  private Header(String key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  /**
   * Creates a new header with the given key and value.
   *
   * @param key The key of the header
   * @param value The value of the header
   * @return The new header
   * @throws IllegalArgumentException If the key or the value are null or empty
   */
  public static Header header(String key, String value) {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("The key of the header cannot be null or empty");
    }
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("The value of the header cannot be null or empty");
    }
    return new Header(key, value.getBytes());
  }

  /**
   * Creates a new header with the given key and value.
   *
   * @param key The key of the header
   * @param value The value of the header
   * @return The new header
   * @throws IllegalArgumentException If the key or the value are null or empty
   */
  public static Header header(String key, byte[] value) {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("The key of the header cannot be null or empty");
    }
    if (value == null || value.length == 0) {
      throw new IllegalArgumentException("The value of the header cannot be null or empty");
    }
    return new Header(key, value);
  }

  public String getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Header header = (Header) o;
    return Objects.equals(key, header.key) && Arrays.equals(value, header.value);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  public org.apache.kafka.common.header.Header toKafkaHeader() {
    return new RecordHeader(key, value);
  }

  @Override
  public String toString() {
    return "Header{" + "key='" + key + '\'' + ", value=" + Arrays.toString(value) + '}';
  }
}
