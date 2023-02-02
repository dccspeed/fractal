package br.ufmg.cs.systems.fractal.subgraph;

public class VertexInducedSubgraphNoEdgeUpdate extends VertexInducedSubgraph {

   public VertexInducedSubgraphNoEdgeUpdate() {
      super();
   }

   @Override
   public void addWord(int word) {
      vertices.add(word);
   }

   @Override
   public void removeLastWord() {
      vertices.removeLast();
   }

   @Override
   public String toString() {
      return "vnoeupdate" + super.toString();
   }

}
