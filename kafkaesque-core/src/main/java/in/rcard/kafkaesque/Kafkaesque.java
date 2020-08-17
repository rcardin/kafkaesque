package in.rcard.kafkaesque;

import static org.reflections.ReflectionUtils.getAllConstructors;
import static org.reflections.ReflectionUtils.withParameters;
import static org.reflections.ReflectionUtils.withParametersCount;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.stream.Stream;
import org.reflections.Reflections;

public interface Kafkaesque {

  // TODO There is a problem with the first type parameter. Try to resolve
  <Key, Value> KafkaesqueConsumer.Builder<?, Key, Value> consume();

  <Key, Value> KafkaesqueProducer.Builder<?, Key, Value> produce();

  static <K> Kafkaesque newInstance(K embeddedKafka) {
    final Set<Class<? extends Kafkaesque>> kafkaesqueClasses = findClassesImplementingKafkaesque();
    validateKafkaesqueClasses(kafkaesqueClasses);
    return kafkaesqueClasses.stream()
        .flatMap(clazz -> findConstructor(embeddedKafka, clazz))
        .findFirst()
        .map(ctor -> buildNewKafkaesqueInstance(embeddedKafka, ctor))
        .orElseThrow(Kafkaesque::makeNoConstructorFoundAssertionError);
  }

  static AssertionError makeNoConstructorFoundAssertionError() {
    return new AssertionError("No method found to build a new instance of the Kafkaesque class");
  }

  static <K> Kafkaesque buildNewKafkaesqueInstance(K embeddedKafka, Constructor<?> ctor) {
    final Object kafkaesque;
    try {
      kafkaesque = ctor.newInstance(embeddedKafka);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new AssertionError(
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

  private static Set<Class<? extends Kafkaesque>> findClassesImplementingKafkaesque() {
    final Reflections reflections = new Reflections("in.rcard.kafkaesque");
    return reflections.getSubTypesOf(Kafkaesque.class);
  }

  private static void validateKafkaesqueClasses(
      Set<Class<? extends Kafkaesque>> kafkaesqueClasses) {
    verifyIfAnyKafkaesqueClassWasFound(kafkaesqueClasses);
    verifyIfMoreThanOneKafkaesqueClassWasFound(kafkaesqueClasses);
  }

  private static void verifyIfAnyKafkaesqueClassWasFound(
      Set<Class<? extends Kafkaesque>> kafkaesqueClasses) {
    if (kafkaesqueClasses == null || kafkaesqueClasses.size() == 0) {
      throw new AssertionError("No implementation of a Kafkaesque class was found");
    }
  }

  private static void verifyIfMoreThanOneKafkaesqueClassWasFound(
      Set<Class<? extends Kafkaesque>> kafkaesqueClasses) {
    if (kafkaesqueClasses.size() > 1) {
      throw new AssertionError(
          String.format(
              "There is more than one implementation of the Kafkaesque main class %s",
              kafkaesqueClasses.toString()));
    }
  }
}
