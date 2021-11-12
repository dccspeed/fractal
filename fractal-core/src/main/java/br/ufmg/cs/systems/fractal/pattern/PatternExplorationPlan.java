package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.util.EdgePredicate;
import br.ufmg.cs.systems.fractal.util.EdgePredicates;
import br.ufmg.cs.systems.fractal.util.VertexPredicate;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import org.apache.log4j.Logger;

import java.io.*;

public class PatternExplorationPlan implements Externalizable {
   private static final Logger LOG = Logger.getLogger(PatternExplorationPlan.class);

   protected ObjArrayList<IntArrayList> intersectionIdxs;
   protected ObjArrayList<IntArrayList> differenceIdxs;
   protected ObjArrayList<VertexPredicate> vertexPredicates;
   protected ObjArrayList<EdgePredicates> edgePredicates;
   protected ObjArrayList<IntArrayList> vertexToEdges;

   public PatternExplorationPlan() {
      this.intersectionIdxs = new ObjArrayList<>();
      this.differenceIdxs = new ObjArrayList<>();
      this.vertexPredicates = new ObjArrayList<>();
      this.edgePredicates = new ObjArrayList<>();
      this.vertexToEdges = new ObjArrayList<>();
   }

   public void init(Pattern pattern) {
      MainGraph graph = pattern.getConfig().getMainGraph();
      for (int i = 0; i < vertexPredicates.size(); ++i) {
         vertexPredicates.get(i).setGraph(graph);
         for (int j = 0; j < edgePredicates.get(i).size(); ++j) {
            edgePredicates.get(i).get(j).setGraph(graph);
         }
      }
   }

   public boolean isEmpty() {
      return intersectionIdxs.isEmpty();
   }

   protected void reset(Pattern pattern) {
      int numVertices = pattern.getNumberOfVertices();
      boolean vertexLabeled = pattern.vertexLabeled();
      intersectionIdxs.clear();
      differenceIdxs.clear();
      vertexToEdges.clear();
      for (int i = 0; i < numVertices; ++i) {
         intersectionIdxs.add(IntArrayListPool.instance().createObject());
         differenceIdxs.add(IntArrayListPool.instance().createObject());
         vertexPredicates.add(vertexLabeled ? new VertexPredicate() : VertexPredicate.trueVertexPredicate);
         edgePredicates.add(new EdgePredicates());
         vertexToEdges.add(new IntArrayList());
      }
   }

   public IntArrayList intersection(int i) {
      return intersectionIdxs.get(i);
   }

   public IntArrayList difference(int i) {
      return differenceIdxs.get(i);
   }

   public IntArrayList vertexEdges(int i) {
      return vertexToEdges.getu(i);
   }

   public VertexPredicate vertexPredicate(int i) {
      return vertexPredicates.get(i);
   }

   public void setVertexPredicate(VertexPredicate vertexPredicate, int i) {
      this.vertexPredicates.set(i, vertexPredicate);
   }

   public EdgePredicates edgePredicates(int i) {
      return edgePredicates.get(i);
   }

   public void updateWithNaivePlan(Pattern pattern) {
      int numVertices = pattern.getNumberOfVertices();

      reset(pattern);

      vertexPredicates.get(0).setLabel(pattern.getFirstVertexLabel());

      for (PatternEdge pedge : pattern.getEdges()) {
         intersectionIdxs.get(pedge.getDestPos()).add(pedge.getSrcPos());
         vertexPredicates.get(pedge.getDestPos()).setLabel(pedge.getDestLabel());
         vertexPredicates.get(pedge.getSrcPos()).setLabel(pedge.getSrcLabel());
         EdgePredicate edgePredicate = new EdgePredicate();
         edgePredicate.setLabel(pedge.getLabel());
         edgePredicates.get(pedge.getDestPos()).add(edgePredicate);
      }

      if (pattern.induced()) {
         for (int dst = 1; dst < numVertices; ++dst) {
            for (int src = 0; src < dst; ++src) {
               if (!intersectionIdxs.get(dst).contains(src)) {
                  differenceIdxs.get(dst).add(src);
               }
            }
         }
      }

      // set vertex to edges mapping
      PatternEdgeArrayList pedges = pattern.getEdges();
      for (int u = 1; u < numVertices; ++u) {
         for (int i = 0; i < pedges.size(); ++i) {
            PatternEdge pedge = pedges.get(i);
            if (pedge.getDestPos() == u) {
               vertexToEdges.get(u).add(i);
            }
         }
      }

      pattern.updateSymmetryBreaker();
   }

   public int mcvcSize() {
      return -1;
   }

   public int numOrderings() {
      return -1;
   }

   public IntArrayList ordering(int i) {
      return null;
   }

   public static ObjArrayList<Pattern> apply(Pattern pattern) {
      PatternExplorationPlan explorationPlan = new PatternExplorationPlan();
      Pattern newPattern = pattern.copy();
      newPattern.setInduced(pattern.induced());
      newPattern.setVertexLabeled(pattern.vertexLabeled());
      PatternUtils.increasingPositions(newPattern);
      explorationPlan.updateWithNaivePlan(newPattern);
      newPattern.setExplorationPlan(explorationPlan);
      ObjArrayList<Pattern> patterns = new ObjArrayList<>();
      patterns.add(newPattern);
      return patterns;
   }

   @Override
   public String toString() {
      return "intersections=" + intersectionIdxs +
              ", differences=" + differenceIdxs +
              ", vertexPredicates=" + vertexPredicates +
              ", edgePredicates=" + edgePredicates;
   }

   public void write(DataOutput out) throws IOException {
      out.writeInt(intersectionIdxs.size());
      for (int i = 0; i < intersectionIdxs.size(); ++i) {
         intersectionIdxs.get(i).write(out);
         differenceIdxs.get(i).write(out);
         if (vertexPredicates.get(i) == VertexPredicate.trueVertexPredicate) {
            out.writeBoolean(true);
         } else {
            out.writeBoolean(false);
            vertexPredicates.get(i).write(out);
         }
         edgePredicates.get(i).write(out);
         vertexToEdges.get(i).write(out);
      }
   }

   public void readFields(DataInput in) throws IOException {
      int numVertices = in.readInt();
      for (int i = 0; i < numVertices; ++i) {
         IntArrayList intersection = new IntArrayList();
         intersection.readFields(in);
         intersectionIdxs.add(intersection);
         IntArrayList difference = new IntArrayList();
         difference.readFields(in);
         differenceIdxs.add(difference);

         boolean trueVertexPredicate = in.readBoolean();
         VertexPredicate vertexPredicate;
         if (trueVertexPredicate) {
            vertexPredicate = VertexPredicate.trueVertexPredicate;
         } else {
            vertexPredicate = new VertexPredicate();
            vertexPredicate.readFields(in);
         }
         vertexPredicates.add(vertexPredicate);

         EdgePredicates vertexEdgePredicates = new EdgePredicates();
         vertexEdgePredicates.readFields(in);
         edgePredicates.add(vertexEdgePredicates);

         // vertex to edge mapping
         IntArrayList vertexEdges = new IntArrayList();
         vertexEdges.readFields(in);
         vertexToEdges.add(vertexEdges);
      }

   }

   @Override
   public void writeExternal(ObjectOutput objectOutput) throws IOException {
      write(objectOutput);
   }

   @Override
   public void readExternal(ObjectInput objectInput) throws IOException {
      readFields(objectInput);
   }
}
