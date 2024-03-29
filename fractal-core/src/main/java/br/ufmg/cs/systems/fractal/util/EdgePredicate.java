package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.graph.MainGraph;

import java.io.*;
import java.util.function.IntPredicate;

public class EdgePredicate implements IntPredicate, Externalizable {
   public static final EdgePredicate.DefaultEdgePredicate trueEdgePredicate =
           new EdgePredicate.DefaultEdgePredicate();

   private int edgeLabel;
   private MainGraph graph;

   public void setLabel(int edgeLabel) {
      this.edgeLabel = edgeLabel;
   }

   public void setGraph(MainGraph graph) {
      this.graph = graph;
   }

   @Override
   public boolean test(int e) {
      return graph.firstEdgeLabel(e) == edgeLabel;
   }

   public void write(DataOutput out) throws IOException {
      out.writeInt(edgeLabel);
   }

   public void readFields(DataInput in) throws IOException {
      edgeLabel = in.readInt();
   }

   @Override
   public void writeExternal(ObjectOutput objectOutput) throws IOException {
      write(objectOutput);
   }

   @Override
   public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
      readFields(objectInput);
   }

   @Override
   public String toString() {
      return "epred{" + edgeLabel + "}";
   }

   private static class DefaultEdgePredicate extends EdgePredicate {
      @Override
      public boolean test(int u) {
         return true;
      }

      @Override
      public String toString() {
         return "epred{true}";
      }
   }
}

