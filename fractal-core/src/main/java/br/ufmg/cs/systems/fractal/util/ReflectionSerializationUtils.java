package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.conf.SparkConfiguration;
import org.apache.log4j.Logger;

import java.io.*;

public class ReflectionSerializationUtils {
   private static final Logger LOG = Logger.getLogger(
           ReflectionSerializationUtils.class);

   public static <T> T newInstance(Class<T> clazz) {
      T object = null;
      try {
          object = clazz.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
          throw new RuntimeException(e + " " + clazz);
      }
      return object;
   }

   public static <T extends Serializable> byte[] serialize(T obj) {
      try {
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos);
         oos.writeObject(obj);
         return baos.toByteArray();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   public static <T extends Serializable> T deserialize(byte[] barray) {
      try {
         ByteArrayInputStream bais = new ByteArrayInputStream(barray);
         ObjectInputStream ois = new ObjectInputStream(bais);
         return (T) ois.readObject();
      } catch (IOException | ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   public static <T extends Serializable> T clone(T obj) {
      return deserialize(serialize(obj));
   }
}
