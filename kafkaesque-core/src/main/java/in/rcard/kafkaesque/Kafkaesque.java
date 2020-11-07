package in.rcard.kafkaesque;

import static org.reflections.ReflectionUtils.getAllConstructors;
import static org.reflections.ReflectionUtils.withParameters;
import static org.reflections.ReflectionUtils.withParametersCount;

import in.rcard.kafkaesque.KafkaesqueProducer.Builder;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.reflections.Reflections;

public interface Kafkaesque {

  <Key, Value> KafkaesqueConsumer.Builder<Key, Value> consume();

  <Key, Value> KafkaesqueProducer.Builder<Key, Value> produce();

  default <Key, Value> KafkaesqueInputTopic<Key, Value> createInputTopic(
      String topic, Serializer<Key> keySerializer, Serializer<Value> valueSerializer) {
    final Builder<Key, Value> builder =
        this.<Key, Value>produce()
            .toTopic(topic)
            .withSerializers(keySerializer, valueSerializer);
    return new KafkaesqueInputTopic<>(builder);
  }

  default <Key, Value> KafkaesqueOutputTopic<Key, Value> createOutputTopic(
      String topic, Deserializer<Key> keyDeserializer, Deserializer<Value> valueDeserializer) {
    this.<Key, Value>consume()
        .fromTopic(topic)
        .withDeserializers(keyDeserializer, valueDeserializer);
    // TODO
    return null;
  }

  static <K> Kafkaesque usingBroker(K embeddedKafka) {
    final Set<Class<? extends Kafkaesque>> kafkaesqueClasses = findClassesImplementingKafkaesque();
    validateKafkaesqueClasses(kafkaesqueClasses);
    return kafkaesqueClasses.stream()
        .flatMap(clazz -> findConstructor(embeddedKafka, clazz))
        .findFirst()
        .map(ctor -> buildNewKafkaesqueInstance(embeddedKafka, ctor))
        .orElseThrow(Kafkaesque::makeNoConstructorFoundException);
  }

  static IllegalStateException makeNoConstructorFoundException() {
    return new IllegalStateException(
        "No method found to build a new instance of the Kafkaesque class");
  }

  static <K> Kafkaesque buildNewKafkaesqueInstance(K embeddedKafka, Constructor<?> ctor) {
    final Object kafkaesque;
    try {
      kafkaesque = ctor.newInstance(embeddedKafka);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "There is no possible to instantiate a new object of type Kafkaesque", e);
    }
    return (Kafkaesque) kafkaesque;
  }

  @SuppressWarnings("rawtypes")
  static <K> Stream<Constructor> findConstructor(
      K embeddedKafka, Class<? extends Kafkaesque> clazz) {
    //noinspection unchecked
    return getAllConstructors(
        clazz, withParameters(embeddedKafka.getClass()), withParametersCount(1))
        .stream();
  }

  static Set<Class<? extends Kafkaesque>> findClassesImplementingKafkaesque() {
    final Reflections reflections = new Reflections("in.rcard.kafkaesque");
    return reflections.getSubTypesOf(Kafkaesque.class);
  }

  static void validateKafkaesqueClasses(Set<Class<? extends Kafkaesque>> kafkaesqueClasses) {
    verifyIfAnyKafkaesqueClassWasFound(kafkaesqueClasses);
    verifyIfMoreThanOneKafkaesqueClassWasFound(kafkaesqueClasses);
  }

  static void verifyIfAnyKafkaesqueClassWasFound(
      Set<Class<? extends Kafkaesque>> kafkaesqueClasses) {
    if (kafkaesqueClasses == null || kafkaesqueClasses.size() == 0) {
      throw new IllegalStateException("No implementation of a Kafkaesque class was found");
    }
  }

  static void verifyIfMoreThanOneKafkaesqueClassWasFound(
      Set<Class<? extends Kafkaesque>> kafkaesqueClasses) {
    if (kafkaesqueClasses.size() > 1) {
      throw new IllegalStateException(
          String.format(
              "There is more than one implementation of the Kafkaesque main class %s",
              kafkaesqueClasses.toString()));
    }
  }
}
