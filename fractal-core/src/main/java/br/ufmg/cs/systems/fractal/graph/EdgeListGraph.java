package br.ufmg.cs.systems.fractal.graph;

import org.apache.commons.io.input.BOMInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.StringTokenizer;

public class EdgeListGraph<V,E> extends BasicMainGraph<V,E> {
   
   public EdgeListGraph(String name) {
      super(name, false, false);
   }

   public EdgeListGraph(String name, boolean isEdgeLabelled,
         boolean isMultiGraph) {
      super(name, isEdgeLabelled, isMultiGraph);
   }

   public EdgeListGraph(Path filePath, boolean isEdgeLabelled,
         boolean isMultiGraph) throws IOException {
      super(filePath.getFileName().toString(), isEdgeLabelled, isMultiGraph);
   }

   public EdgeListGraph(org.apache.hadoop.fs.Path hdfsPath,
         boolean isEdgeLabelled, boolean isMultiGraph) throws IOException {
      super(hdfsPath.getName(), isEdgeLabelled, isMultiGraph);
   }

   @Override
   protected void readFromInputStream(InputStream is) {
      try {
         BufferedReader reader = new BufferedReader(
               new InputStreamReader(new BOMInputStream(is)));

         String line = reader.readLine();

         while (line != null) {
            if (line.startsWith("#")) {
               line = reader.readLine();
               continue;
            }

            StringTokenizer tokenizer = new StringTokenizer(line);

            Vertex vertex = parseVertex(tokenizer);
            int vertexId = vertex.getVertexId();

            while (tokenizer.hasMoreTokens()) {
               Edge edge = parseEdge(tokenizer, vertexId);
               addEdge(edge);
            }

            line = reader.readLine();
         }

         reader.close();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   protected Vertex parseVertex(StringTokenizer tokenizer) {
      int vertexId = Integer.parseInt(tokenizer.nextToken());

      int vertexIdx = vertexIdMap.get(vertexId);
      if (vertexIdx == -1) {
         vertexIdx = vertexIdMap.size();
         vertexIdMap.put(vertexId, vertexIdx);
         Vertex vertex = createVertex(vertexIdx, vertexId, 1);
         addVertex(vertex);
         return vertex;
      } else {
         return vertexIndexF[vertexIdx];
      }
   }

   @Override
   protected Edge parseEdge(StringTokenizer tokenizer, int vertexId) {
      Vertex neighborVertex = parseVertex(tokenizer);
      int neighborId = neighborVertex.getVertexId();

      if (!isEdgeLabelled) {
         int from, to;
         if (vertexId < neighborId) {
            from = vertexId;
            to = neighborId;
         } else {
            from = neighborId;
            to = vertexId;
         }
         return createEdge(from, to);
      } else {
         throw new RuntimeException(
               "Edge label is not allowed in edge list format");
      }
   }
}
