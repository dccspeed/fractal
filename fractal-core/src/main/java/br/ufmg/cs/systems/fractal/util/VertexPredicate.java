package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.graph.MainGraph;

import java.io.*;
import java.util.function.IntPredicate;

public class VertexPredicate implements IntPredicate, Externalizable {

   public static final DefaultVertexPredicate trueVertexPredicate = new DefaultVertexPredicate();

   private MainGraph graph;
   private int vertexLabel = 1;

   public void setLabel(int vertexLabel) {
      this.vertexLabel = vertexLabel;
   }

   public void setGraph(MainGraph graph) {
      this.graph = graph;
   }

   @Override
   public boolean test(int u) {
      return graph.firstVertexLabel(u) == vertexLabel;
   }

   public void write(DataOutput out) throws IOException {
      out.writeInt(vertexLabel);
   }

   public void readFields(DataInput in) throws IOException {
      vertexLabel = in.readInt();
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
      return "vpred{" + vertexLabel + "}";
   }

   private static class DefaultVertexPredicate extends VertexPredicate {
      @Override
      public boolean test(int u) {
         return true;
      }

      @Override
      public String toString() {
         return "vpred{true}";
      }
   }

}
