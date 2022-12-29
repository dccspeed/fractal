package br.ufmg.cs.systems.fractal.pattern;

import java.io.*;
import java.util.ArrayList;
import java.util.function.Supplier;

import br.ufmg.cs.systems.fractal.pattern.pool.PatternEdgePool;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultUndirectedGraph;
import org.jgrapht.graph.SimpleGraph;
import org.jgrapht.graph.builder.GraphBuilder;
import org.jgrapht.nio.graph6.Graph6Sparse6Importer;
import org.jgrapht.opt.graph.sparse.SparseIntUndirectedGraph;

public class NautyPatternIterator {
   private static final String cmd = "/home/viniciusvdias/repos/nauty27/geng " +
           "-c";

   private int numVertices;

   private BufferedReader input;
   private BufferedWriter output;

   private String lastLine;

   private PatternEdgePool edgePool = PatternEdgePool.instance(false);

   private Pattern pattern = PatternUtils.emptyPattern();

   private Graph6Sparse6Importer importer = new Graph6Sparse6Importer<>();

   public NautyPatternIterator(int numVertices) throws IOException {
      this.numVertices = numVertices;
      Process p = Runtime.getRuntime().exec(cmd + " " + numVertices);
      input = new BufferedReader(new InputStreamReader(p.getInputStream()));
      output = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()));
   }

   public boolean hasNext() throws IOException {
      lastLine = input.readLine();
      if (lastLine == null) return false;
      else if (lastLine.equals("WAIT")) {
         output.write("\n");
         output.flush();
         return hasNext();
      } else return true;
   }

   public Pattern next() {
      String graphStr = lastLine;
      DefaultUndirectedGraph<Integer,Integer> graph =
              new DefaultUndirectedGraph<>(new ResetSupplier(),
                      new ResetSupplier(), false);
      importer.importGraph(graph, new StringReader(graphStr));

      pattern.reset();
      for (int i = 0; i < numVertices; ++i) {
         pattern.addVertexStandalone();
      }

      for (Integer e : graph.edgeSet()) {
         Integer src = graph.getEdgeSource(e);
         Integer dst = graph.getEdgeTarget(e);
         PatternEdge edge = edgePool.createObject();
         edge.setSrcPos(src.intValue());
         edge.setDestPos(dst.intValue());
         pattern.addEdgeStandalone(edge);
      }

      return pattern;
   }

   public void close() throws IOException {
      if (input != null) input.close();
      if (output != null) output.close();
   }

   public static void main(String[] args) throws IOException {
      System.out.println("BeforeProcess");
      NautyPatternIterator it = new NautyPatternIterator(10);

      int n = 0;
      while (it.hasNext()) {
         System.out.println(n + " pattern: " + it.next());
         ++n;
      }

      it.close();

   }

   private class ResetSupplier implements Supplier<Integer> {

      private int count = 0;

      @Override
      public Integer get() {
         return count++;
      }
   }
}
