package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.graph.MainGraph;

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

    @Override
    public void setFromOther(PatternEdge edge) {
        super.setFromOther(edge);

        if (edge instanceof LabelledPatternEdge) {
            label = ((LabelledPatternEdge) edge).label;
        }
    }

    @Override
    public void setFromEdge(MainGraph mainGraph, int edgeId, int srcPos, int dstPos, int srcId) {
        super.setFromEdge(mainGraph, edgeId, srcPos, dstPos, srcId);
        label = mainGraph.firstEdgeLabel(edgeId);
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
