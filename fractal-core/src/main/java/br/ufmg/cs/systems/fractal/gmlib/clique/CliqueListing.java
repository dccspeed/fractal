package br.ufmg.cs.systems.fractal.gmlib.clique;

import br.ufmg.cs.systems.fractal.util.Utils;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.map.IntObjCursor;
import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.function.IntObjConsumer;
import org.apache.commons.io.input.BOMInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;

public class CliqueListing {

   protected long cliqueCount;

   protected CounterConsumer counterConsumer;
   
   protected GraphConsumer[] graphConsumers;

   private static HashIntObjMap<IntArrayList> readFromInputStream(
         InputStream is) throws IOException {
      HashIntObjMap<IntArrayList> adjLists = HashIntObjMaps.newMutableMap();

      BufferedReader reader = new BufferedReader(
            new InputStreamReader(new BOMInputStream(is)));
      
      String line = reader.readLine();

      while (line != null) {
         if (line.startsWith("#")) {
            line = reader.readLine();
            continue;
         }
         StringTokenizer tokenizer = new StringTokenizer(line);

         int v = Integer.parseInt(tokenizer.nextToken());
         int label = Integer.parseInt(tokenizer.nextToken());

         IntArrayList neighbors = adjLists.get(v);
         
         if (neighbors == null) {
            neighbors = IntArrayListPool.instance().createObject();
            adjLists.put(v, neighbors);
         }

         while (tokenizer.hasMoreTokens()) {
            int u = Integer.parseInt(tokenizer.nextToken());
            if (u > v) {
               neighbors.add(u);
            }
         }

         line = reader.readLine();
      }

      IntObjCursor<IntArrayList> cur = adjLists.cursor();
      while (cur.moveNext()) {
         cur.value().sort();
      }

      return adjLists;
   }

   private void cliqueListing(int l, HashIntObjMap<IntArrayList> graph) {
      if (l == 2) {
         graph.forEach(counterConsumer);
         return;
      }

      GraphConsumer consumer = graphConsumers[l];
      consumer.set(l, graph);
      graph.forEach(consumer);
   }

   protected class CounterConsumer implements IntObjConsumer<IntArrayList> {
      @Override
      public void accept(int u, IntArrayList uneighbors) {
         for (int i = 0; i < uneighbors.size(); ++i) {
            CliqueListing.this.cliqueCount++;
         }
      }
   }

   protected class GraphConsumer implements IntObjConsumer<IntArrayList> {
      private int l;
      private HashIntObjMap<IntArrayList> graph;

      public void set(int l, HashIntObjMap<IntArrayList> graph) {
         this.l = l;
         this.graph = graph;
      }

      @Override
      public void accept(int u, IntArrayList uneighbors) {
         HashIntObjMap ugraph = HashIntObjMaps.newMutableMap();
         for (int i = 0; i < uneighbors.size(); ++i) {
            int v = uneighbors.get(i);
            IntArrayList vneighbors = graph.get(v);
            IntArrayList target = IntArrayListPool.instance().createObject();
            Utils.sintersect(uneighbors, vneighbors,
                  i + 1, uneighbors.size(), 0, vneighbors.size(), target);
            ugraph.put(v, target);
         }

         CliqueListing.this.cliqueListing(l - 1, ugraph);
      }
   }

   public static void main(String[] args) throws IOException {
      int cliqueSize = -1;
      String inputPath = null;

      try {
         cliqueSize = Integer.parseInt(args[0]);
         inputPath = args[1];
      } catch (Exception e) {
         System.out.println(
               "Usage: java br.ufmg.cs.systems.fractal.gmlib.clique.CliqueListing <cliqueSize> <inputGraph>");
         System.exit(1);
      }

      Paths.get(inputPath);
      InputStream is = Files.newInputStream(Paths.get(inputPath));
      HashIntObjMap<IntArrayList> graph = readFromInputStream(is);
      is.close();

      CliqueListing cl = new CliqueListing();

      cl.cliqueCount = 0;
      cl.counterConsumer = cl.new CounterConsumer();
      cl.graphConsumers = new GraphConsumer[cliqueSize + 1];
      for (int i = 0; i < cl.graphConsumers.length; ++i) {
         cl.graphConsumers[i] = cl.new GraphConsumer();
      }

      long start = System.currentTimeMillis();

      cl.cliqueListing(cliqueSize, graph);

      long elapsed = System.currentTimeMillis() - start;

      System.out.printf("CliqueListing %d %d %d\n",
            cliqueSize, cl.cliqueCount, elapsed);

   }
}
