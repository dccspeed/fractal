package io.arabesque.utils.collection;

import io.arabesque.conf.SparkConfiguration$;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

public class BoundedPriorityQueue<T extends Writable> implements Writable {
   protected int capacity;
   protected PriorityQueue<T> queue;
   protected Comparator<T> comparator;
   
   public BoundedPriorityQueue() {}

   public void init(int capacity, Comparator<T> comparator) {
      this.capacity = capacity;
      this.queue = new PriorityQueue<T>(capacity + 1, comparator);
      this.comparator = comparator;
   }

   public int size() {
      return queue != null ? queue.size() : 0;
   }

   public void clear() {
      if (queue != null) queue.clear();
   }

   public boolean offer(T e) {
      if (size() < capacity) {
         queue.offer(e);
         return true;
      } else if (comparator.compare(e, queue.peek()) <= 0) {
         queue.poll();
         queue.offer(e);
         return true;
      }

      return false;
   }

   public T peek() {
      return queue != null ? queue.peek() : null;
   }

   public T poll() {
      return queue != null ? queue.poll() : null;
   }

   public void merge(BoundedPriorityQueue<T> other) {
      T elem = other.poll();
      while (elem != null) {
         queue.offer(elem);
         elem = other.poll();
      }
   }
    
   @Override
   public void write(DataOutput out) throws IOException {
      out.writeInt(capacity);

      byte[] comparatorBytes = SparkConfiguration$.MODULE$.serialize(comparator);
      out.writeInt(comparatorBytes.length);
      out.write(comparatorBytes);

      int size = size();
      out.writeInt(size);

      if (size > 0) {
         T elem = poll();
         Text.writeString(out, elem.getClass().getName());
         while (elem != null) {
            elem.write(out);
            elem = poll();
         }
      }
   }
    
   @Override
   public void readFields(DataInput in) throws IOException {
      capacity = in.readInt();

      int byteSize = in.readInt();
      byte[] comparatorBytes = new byte[byteSize];
      in.readFully(comparatorBytes);
      comparator = (Comparator<T>) SparkConfiguration$.MODULE$.deserialize(comparatorBytes);
      queue = new PriorityQueue<T>(capacity + 1, comparator);

      int size = in.readInt();

      if (size > 0) {
         try {
            Class<? extends Writable> eclass = Class.forName(Text.readString(in)).
               asSubclass(Writable.class);
            for (int i = 0; i < size; ++i) {
               T elem = (T) eclass.newInstance();
               elem.readFields(in);
               offer(elem);
            }
         } catch (InstantiationException e) {
            throw new IOException("Failed element init", e);
         } catch (ClassNotFoundException e) {
            throw new IOException("Failed element init", e);
         } catch (IllegalAccessException e) {
            throw new IOException("Failed element init", e);
         }
      }
   }
}
