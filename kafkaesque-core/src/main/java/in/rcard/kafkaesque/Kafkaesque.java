package in.rcard.kafkaesque;

import static org.reflections.ReflectionUtils.getAllMethods;
import static org.reflections.ReflectionUtils.withParametersCount;
import static org.reflections.ReflectionUtils.withReturnTypeAssignableTo;

import in.rcard.kafkaesque.KafkaesqueConsumer.Builder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.stream.Stream;
import org.reflections.Reflections;

public class Kafkaesque {

  private Kafkaesque() {
    // Empty body
  }

  private static Kafkaesque newInstance() {
    return new Kafkaesque();
  }
  
  private KafkaesqueConsumer.Builder consume() {
    final Set<Class<? extends Builder>> buildersClass = findClassesImplementingBuilder();
    validateBuilderClasses(buildersClass);
    return buildersClass.stream()
        .flatMap(this::findFactoryMethods)
        .findFirst()
        .map(this::invokeTheFactoryMethod)
        .orElseThrow(() -> new AssertionError("TODO"));
  }
  
  private void validateBuilderClasses(Set<Class<? extends Builder>> buildersClass) {
    verifyIfAnyBuilderClassWasFound(buildersClass);
    verifyIfMoreThanOneBuilderClassWasFound(buildersClass);
  }
  
  private void verifyIfMoreThanOneBuilderClassWasFound(
      Set<Class<? extends Builder>> buildersClass) {
    if (buildersClass.size() > 1) {
      throw new AssertionError(
          String.format(
              "There is more than one implementation of the Kafkaesque consumer %s",
              buildersClass.toString()));
    }
  }
  
  private void verifyIfAnyBuilderClassWasFound(Set<Class<? extends Builder>> buildersClass) {
    if (buildersClass == null || buildersClass.size() == 0) {
      throw new AssertionError("No implementation of a Kafkaesque consumer found");
    }
  }
  
  private Set<Class<? extends Builder>> findClassesImplementingBuilder() {
    final Reflections reflections = new Reflections("in.rcard.kafkaesque");
    return reflections.getSubTypesOf(Builder.class);
  }
  
  private Builder invokeTheFactoryMethod(Method method) {
    try {
      final Object returnedObject = method.invoke(null);
      return (Builder) returnedObject;
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new AssertionError("TODO");
    }
  }
  
  private Stream<Method> findFactoryMethods(Class<? extends Builder> builderClass) {
    //noinspection unchecked
    return getAllMethods(
        builderClass, withReturnTypeAssignableTo(KafkaesqueConsumer.class), withParametersCount(0))
               .stream();
  }
}
