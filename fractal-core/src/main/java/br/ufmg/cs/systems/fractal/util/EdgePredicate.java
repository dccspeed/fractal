package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.graph.MainGraph;
import com.koloboke.function.IntIntPredicate;
import org.apache.hadoop.io.Writable;
import sun.applet.Main;

import java.io.*;
import java.util.function.IntPredicate;

public class EdgePredicate implements IntPredicate, Writable, Externalizable {
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
      return graph.edgeLabel(e) == edgeLabel;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      out.writeInt(edgeLabel);
   }

   @Override
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
}

