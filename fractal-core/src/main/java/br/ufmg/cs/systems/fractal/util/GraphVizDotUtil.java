package br.ufmg.cs.systems.fractal.util;

import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.pattern.PatternEdge;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class GraphVizDotUtil {
   public static void exportPattern(Pattern pattern, String path) {
      FileWriter fw;
      PrintWriter pw = null;
      try {
         fw = new FileWriter(path);
         pw = new PrintWriter(fw);
         pw.printf("graph {\n");
         pw.printf("\tnode [colorscheme=pastel19];\n");

         int numVertices = pattern.getNumberOfVertices();
         IntArrayList vlabels = new IntArrayList();
         IntIntMap colorMap = HashIntIntMaps.newUpdatableMap();

         for (int u = 0; u < numVertices; ++u) vlabels.add(-1);

         for (PatternEdge pe : pattern.getEdges()) {
            int srcLabel = pe.getSrcLabel();
            int dstLabel = pe.getDestLabel();
            vlabels.set(pe.getSrcPos(), srcLabel);
            vlabels.set(pe.getDestPos(), dstLabel);
            if (!colorMap.containsKey(srcLabel)) {
               Random random = new Random(srcLabel);
               int randomColor = (int) (random.nextDouble() * 0x1000000);
               colorMap.put(srcLabel, randomColor);
            }
            if (!colorMap.containsKey(dstLabel)) {
               Random random = new Random(dstLabel);
               int randomColor = (int) (random.nextDouble() * 0x1000000);
               colorMap.put(dstLabel, randomColor);
            }
         }

         for (int u = 0; u < numVertices; ++u) {
            int label = vlabels.get(u);
            pw.printf("\t%d [style=\"filled,solid\"," +
                            "color=black,fillcolor=\"#%06x\"," +
                            "label=\"%d,{%d}\"];" +
                            "\n",
                    u, colorMap.get(label), u, label);
         }

         for (PatternEdge pe : pattern.getEdges()) {
            pw.printf("\t%d -- %d;\n", pe.getSrcPos(), pe.getDestPos());
         }

         pw.printf("}\n");
      } catch (IOException e) {
         e.printStackTrace();
      } finally {
         if (pw != null)  pw.close();
      }
   }
}
