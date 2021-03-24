package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.graph.Vertex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PatternEdge implements Comparable<PatternEdge> {

   /// protected MainGraph mainGraph;
   private int srcPos;
   private int srcLabel;
   private int destPos;
   private int destLabel;

   public PatternEdge() {
      this(null, -1, -1, -1, -1);
   }

   public PatternEdge(PatternEdge edge) {
      setFromOther(edge);
   }

   public PatternEdge(MainGraph mainGraph,
                      int srcPos, int srcLabel, int destPos, int destLabel) {
      this.srcPos = srcPos;
      this.srcLabel = srcLabel;
      this.destPos = destPos;
      this.destLabel = destLabel;
   }

   public void setFromOther(PatternEdge edge) {
      setSrcPos(edge.getSrcPos());
      setSrcLabel(edge.getSrcLabel());

      setDestPos(edge.getDestPos());
      setDestLabel(edge.getDestLabel());
   }

   public void setFromEdge(MainGraph mainGraph, int edgeId, int srcPos, int dstPos, int srcId) {
      int srcVertexId = mainGraph.edgeSrc(edgeId);
      int dstVertexId = mainGraph.edgeDst(edgeId);

      int srcVertexLabel = mainGraph.vertexLabel(srcVertexId);
      int dstVertexLabel = mainGraph.vertexLabel(dstVertexId);

      setSrcLabel(srcVertexLabel);
      setDestLabel(dstVertexLabel);

      if (srcId != srcVertexId) {
         invert();
      }

      setSrcPos(srcPos);
      setDestPos(dstPos);
   }

   public void invert() {
      int tmp = srcPos;
      srcPos = destPos;
      destPos = tmp;

      tmp = srcLabel;
      srcLabel = destLabel;
      destLabel = tmp;
   }

   public int getSrcPos() {
      return srcPos;
   }

   public void setSrcPos(int srcPos) {
      this.srcPos = srcPos;
   }

   public int getSrcLabel() {
      return srcLabel;
   }

   public void setSrcLabel(int srcLabel) {
      this.srcLabel = srcLabel;
   }

   public int getDestPos() {
      return destPos;
   }

   public void setDestPos(int destPos) {
      this.destPos = destPos;
   }

   public int getDestLabel() {
      return destLabel;
   }

   public void setDestLabel(int destLabel) {
      this.destLabel = destLabel;
   }

   public int getLabel() {
      return 0;
   }

   public String toString() {
      return ("(" + srcPos + "," + srcLabel + "-" + destPos + "," + destLabel + ")");
   }

   public void write(DataOutput out) throws IOException {
      out.writeInt(this.srcPos);
      out.writeInt(this.srcLabel);
      out.writeInt(this.destPos);
      out.writeInt(this.destLabel);
   }

   public void readFields(DataInput in) throws IOException {
      this.srcPos = in.readInt();
      this.srcLabel = in.readInt();
      this.destPos = in.readInt();
      this.destLabel = in.readInt();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PatternEdge that = (PatternEdge) o;

      if (srcPos != that.srcPos) return false;
      if (srcLabel != that.srcLabel) return false;
      if (destPos != that.destPos) return false;
      if (destLabel != that.destLabel) return false;
      return true;
   }

   @Override
   public int hashCode() {
      int result = srcPos;
      result = 31 * result + srcLabel;
      result = 31 * result + destPos;
      result = 31 * result + destLabel;
      //result = 31 * result + (isForward ? 1 : 0);
      return result;
   }

   @Override
   public int compareTo(PatternEdge o) {
      if (equals(o)) {
         return 0;
      }

      int result;

      boolean srcPosEqual = this.srcPos == o.getSrcPos();
      boolean dstPosEqual = this.destPos == o.getDestPos();

      if (srcPosEqual && dstPosEqual) {
         if (this.srcLabel == o.getSrcLabel()) {
            result = Integer.compare(destLabel, o.getDestLabel());
         }
         else {
            result = Integer.compare(srcLabel, o.getSrcLabel());
         }
      }
      else if (dstPosEqual) {
         result = -1 * Integer.compare(srcPos, o.getSrcPos());
      }
      else {
         result = Integer.compare(destPos, o.getDestPos());
      }

      return result;
   }
}
