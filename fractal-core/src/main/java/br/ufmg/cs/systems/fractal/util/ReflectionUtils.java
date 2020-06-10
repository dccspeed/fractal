package br.ufmg.cs.systems.fractal.util;

import java.io.IOException;
import java.io.Serializable;

public class ReflectionUtils {
   private static final org.apache.hadoop.conf.Configuration hadoopConf =
           new org.apache.hadoop.conf.Configuration();

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
     T cpy = (T) newInstance(obj.getClass());
      try {
         org.apache.hadoop.util.ReflectionUtils.copy(hadoopConf, obj, cpy);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      return cpy;
   }
}
