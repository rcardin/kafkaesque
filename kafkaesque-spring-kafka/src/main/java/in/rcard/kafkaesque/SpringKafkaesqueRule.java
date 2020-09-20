package in.rcard.kafkaesque;

import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

/**
 * A Junit4 Rule wrapping the {@link EmbeddedKafkaRule}. It creates an instance of the
 * {@link SpringKafkaesque} class. It is recommended to use the rule as a {@code @ClassRule}.
 */
public class SpringKafkaesqueRule extends EmbeddedKafkaRule {
  
  private final SpringKafkaesque kafkaesque;
  
  /**
   * @see EmbeddedKafkaRule#EmbeddedKafkaRule(int)
   */
  public SpringKafkaesqueRule(int count) {
    super(count);
    this.kafkaesque = new SpringKafkaesque(getEmbeddedKafka());
  }
  
  /**
   * @see EmbeddedKafkaRule#EmbeddedKafkaRule(int, boolean, String...)
   */
  public SpringKafkaesqueRule(int count, boolean controlledShutdown, String... topics) {
    super(count, controlledShutdown, topics);
    this.kafkaesque = new SpringKafkaesque(getEmbeddedKafka());
  }
  
  /**
   * @see EmbeddedKafkaRule#EmbeddedKafkaRule(int, boolean, int, String...)
   */
  public SpringKafkaesqueRule(int count, boolean controlledShutdown, int partitions,
      String... topics) {
    super(count, controlledShutdown, partitions, topics);
    this.kafkaesque = new SpringKafkaesque(getEmbeddedKafka());
  }
  
  @Override
  public void before() {
    super.before();
  }
  
  @Override
  public void after() {
    super.after();
  }
  
  /**
   * Returns the {@link SpringKafkaesque} instance.
   * @return A SpringKafkaesque instance
   */
  public Kafkaesque getKafkaesque() {
    return kafkaesque;
  }
}
