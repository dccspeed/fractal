package br.ufmg.cs.systems.fractal.gmlib.keywordsearch;

import br.ufmg.cs.systems.fractal.graph.BasicMainGraph;
import com.koloboke.collect.set.hash.HashObjSet;
import com.koloboke.collect.set.hash.HashObjSets;

import java.io.IOException;
import java.nio.file.Path;
import java.util.StringTokenizer;


public class KeywordSearchGraph
      extends BasicMainGraph<HashObjSet<String>,HashObjSet<String>> {
   public KeywordSearchGraph(String name) {
      super(name, false, false);
   }

   public KeywordSearchGraph(String name, boolean isEdgeLabelled,
         boolean isMultiGraph) {
      super(name, isEdgeLabelled, isMultiGraph);
   }

   public KeywordSearchGraph(Path filePath, boolean isEdgeLabelled,
         boolean isMultiGraph) throws IOException {
      super(filePath.getFileName().toString(), isEdgeLabelled, isMultiGraph);
   }

   public KeywordSearchGraph(org.apache.hadoop.fs.Path hdfsPath,
         boolean isEdgeLabelled, boolean isMultiGraph) throws IOException {
      super(hdfsPath.getName(), isEdgeLabelled, isMultiGraph);
   }

   private HashObjSet<String> parseWordSet(StringTokenizer tokenizer) {
      HashObjSet<String> prop = HashObjSets.<String>newMutableSet();
      while (tokenizer.hasMoreTokens()) {
         prop.add(tokenizer.nextToken());
      }

      return prop;
   }
   
   @Override
   protected HashObjSet<String> parseVertexProperty(StringTokenizer tokenizer) {
      return parseWordSet(tokenizer);
   }
   
   @Override
   protected HashObjSet<String> parseEdgeProperty(StringTokenizer tokenizer) {
      return parseWordSet(tokenizer);
   }

}
