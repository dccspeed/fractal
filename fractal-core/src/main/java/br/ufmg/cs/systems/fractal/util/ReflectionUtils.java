package br.ufmg.cs.systems.fractal.util;

public class ReflectionUtils {

  public static <T> T newInstance(Class<T> clazz) {
      T object = null;
      try {
          object = clazz.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
          throw new RuntimeException(e);
      }
      return object;
   }
}
