package in.rcard.kafkaesque.producer;

import static in.rcard.kafkaesque.producer.KafkaesqueProducer.Header.header;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import in.rcard.kafkaesque.producer.KafkaesqueProducer.Header;
import org.junit.jupiter.api.Test;

class HeaderTest {

  private static final String A_KEY = "key";
  private static final String A_VALUE = "value";
  private static final byte[] A_VALUE_BYTES = A_VALUE.getBytes();

  @Test
  void headerWithStringValueShouldCreateAValidHeader() {
    final Header actualHeader = header(A_KEY, A_VALUE);

    assertThat(actualHeader).extracting("key", "value").contains(A_KEY, A_VALUE.getBytes());
  }

  @Test
  void headerWithStringValueShouldThrownAnExceptionIfTheKeyIsNull() {
    assertThatThrownBy(() -> header(null, A_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The key of the header cannot be null or empty");
  }

  @Test
  void headerWithStringValueShouldThrownAnExceptionIfTheKeyIsEmpty() {
    assertThatThrownBy(() -> header("", A_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The key of the header cannot be null or empty");
  }

  @Test
  void headerWithStringValueShouldThrownAnExceptionIfTheValueIsNull() {
    assertThatThrownBy(() -> header(A_KEY, (String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The value of the header cannot be null or empty");
  }

  @Test
  void headerWithStringValueShouldThrownAnExceptionIfTheValueIsEmpty() {
    assertThatThrownBy(() -> header(A_KEY, ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The value of the header cannot be null or empty");
  }

  @Test
  void headerWithBytesValueShouldCreateAValidHeader() {
    final Header actualHeader = header(A_KEY, A_VALUE_BYTES);

    assertThat(actualHeader).extracting("key", "value").contains(A_KEY, A_VALUE_BYTES);
  }

  @Test
  void headerWithBytesValueShouldThrownAnExceptionIfTheKeyIsNull() {
    assertThatThrownBy(() -> header(null, A_VALUE_BYTES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The key of the header cannot be null or empty");
  }

  @Test
  void headerWithBytesValueShouldThrownAnExceptionIfTheKeyIsEmpty() {
    assertThatThrownBy(() -> header("", A_VALUE_BYTES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The key of the header cannot be null or empty");
  }

  @Test
  void headerWithBytesValueShouldThrownAnExceptionIfTheValueIsNull() {
    assertThatThrownBy(() -> header(A_KEY, (byte[]) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The value of the header cannot be null or empty");
  }

  @Test
  void headerWithBytesValueShouldThrownAnExceptionIfTheValueIsEmpty() {
    assertThatThrownBy(() -> header(A_KEY, new byte[0]))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The value of the header cannot be null or empty");
  }
}
