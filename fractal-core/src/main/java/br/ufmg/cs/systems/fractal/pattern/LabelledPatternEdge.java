package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.LabelledEdge;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.pattern.pool.PatternEdgePool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Alex on 08-Nov-15.
 */
public class LabelledPatternEdge extends PatternEdge {
    private int label;

    public LabelledPatternEdge() {
        label = 0;
    }

    public LabelledPatternEdge(LabelledPatternEdge edge) {
        super(edge);

        this.label = edge.label;
    }

    public LabelledPatternEdge(MainGraph mainGraph,
          int srcPos, int srcLabel, int destPos, int destLabel, int label) {
        super(mainGraph, srcPos, srcLabel, destPos, destLabel);

        this.label = label;
    }
    
    @Override
    public void reclaim() {
        PatternEdgePool.instance(true).reclaimObject(this);
    }

    @Override
    public void setFromOther(PatternEdge edge) {
        super.setFromOther(edge);

        if (edge instanceof LabelledPatternEdge) {
            label = ((LabelledPatternEdge) edge).label;
        }
    }

    @Override
    public void setFromEdge(MainGraph mainGraph, Edge edge, int srcPos, int dstPos, int srcId) {
        super.setFromEdge(mainGraph, edge, srcPos, dstPos, srcId);

        if (edge instanceof LabelledEdge) {
            label = ((LabelledEdge) edge).getEdgeLabel();
        }
    }

    @Override
    public int getLabel() {
        return label;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        LabelledPatternEdge that = (LabelledPatternEdge) o;

        return label == that.label;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + label;
        return result;
    }

    @Override
    public int compareTo(PatternEdge o) {
        int result = super.compareTo(o);

        if (result != 0) {
            return result;
        }

        if (o instanceof LabelledPatternEdge) {
            LabelledPatternEdge lo = (LabelledPatternEdge) o;
            return Integer.compare(label, lo.label);
        }

        return 0;
    }

    @Override
    public String toString() {
        return ("[(" + getSrcPos() + "," + getSrcLabel() + "),(" + label + ")-(" + getDestPos() + "," + getDestLabel() + ")]");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(label);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        label = in.readInt();
    }
}
