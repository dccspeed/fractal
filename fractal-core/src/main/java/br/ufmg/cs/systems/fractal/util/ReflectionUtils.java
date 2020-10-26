package br.ufmg.cs.systems.fractal.util;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;

public class ReflectionUtils {
   private static final Logger LOG = Logger.getLogger(ReflectionUtils.class);

   public static <T> T newInstance(Class<T> clazz) {
      T object = null;
      try {
          object = clazz.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
          throw new RuntimeException(e + " " + clazz);
      }
      return object;
   }

   public static <T extends Serializable> T clone(T obj) {
      return SerializationUtils.clone(obj);
   }
}
