package in.rcard.kafkaesque;

import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

/**
 * A wrapper Junit4 Rule to {@link EmbeddedKafkaRule}. It creates an instance of the
 * {@link SpringKafkaesque} class. It is recommended to use the rule as a {@code @ClassRule}.
 */
public class SpringKafkaesqueRule extends EmbeddedKafkaRule {
  
  private final SpringKafkaesque kafkaesque;
  
  public SpringKafkaesqueRule(int count) {
    super(count);
    this.kafkaesque = new SpringKafkaesque(getEmbeddedKafka());
  }
  
  public SpringKafkaesqueRule(int count, boolean controlledShutdown, String... topics) {
    super(count, controlledShutdown, topics);
    this.kafkaesque = new SpringKafkaesque(getEmbeddedKafka());
  }
  
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
  
  public Kafkaesque getKafkaesque() {
    return kafkaesque;
  }
}
