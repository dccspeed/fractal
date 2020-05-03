package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class EdgePredicates extends ObjArrayList<EdgePredicate> implements Externalizable, Writable {
   @Override
   public void write(DataOutput out) throws IOException {
      out.writeInt(size());
      for (int i = 0; i < size(); ++i) {
         get(i).write(out);
      }
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      for (int i = 0; i < size; ++i) {
         EdgePredicate edgePredicate = new EdgePredicate();
         edgePredicate.readFields(in);
         add(edgePredicate);
      }
   }

   @Override
   public void writeExternal(ObjectOutput objectOutput) throws IOException {
      write(objectOutput);
   }

   @Override
   public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
      readFields(objectInput);
   }
}
