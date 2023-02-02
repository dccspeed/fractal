package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;

import java.io.*;

public class EdgePredicates extends ObjArrayList<EdgePredicate> implements Externalizable {

   public static EdgePredicates trueEdgePredicates = new EdgePredicates() {
      @Override
      public EdgePredicate get(int i) {
         return EdgePredicate.trueEdgePredicate;
      }

      @Override
      public EdgePredicate getu(int i) {
         return EdgePredicate.trueEdgePredicate;
      }
   };

   public void write(DataOutput out) throws IOException {
      out.writeInt(size());
      for (int i = 0; i < size(); ++i) {
         EdgePredicate epred = get(i);
         if (epred == EdgePredicate.trueEdgePredicate) {
            out.writeBoolean(true);
         } else {
            out.writeBoolean(false);
            epred.write(out);
         }
      }
   }

   public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      for (int i = 0; i < size; ++i) {
         boolean isDefaultPredicate = in.readBoolean();
         if (isDefaultPredicate) {
            add(EdgePredicate.trueEdgePredicate);
         } else {
            EdgePredicate edgePredicate = new EdgePredicate();
            edgePredicate.readFields(in);
            add(edgePredicate);
         }
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
