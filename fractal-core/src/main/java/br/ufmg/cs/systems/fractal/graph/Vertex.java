package br.ufmg.cs.systems.fractal.graph;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Vertex<V> implements Writable, java.io.Serializable {

   private int vertexOriginalId = -1;

    private int vertexId;
    private int vertexLabel;
    private V property;

    public Vertex() {
        this(0, 0);
    }

    public Vertex(int vertexId, int vertexLabel) {
        this.vertexId = vertexId;
        this.vertexLabel = vertexLabel;
    }
    
    public Vertex(int vertexId, int vertexOriginalId, int vertexLabel) {
        this.vertexId = vertexId;
        this.vertexOriginalId = vertexOriginalId;
        this.vertexLabel = vertexLabel;
    }

    public int getVertexId() {
        return vertexId;
    }

    public int getVertexOriginalId() {
       return vertexOriginalId;
    }

    public int getVertexLabel() {
        return vertexLabel;
    }

    public void setVertexLabel(int vertexLabel) {
       this.vertexLabel = vertexLabel;
    }

    void setProperty(V property) {
       this.property = property;
    }

    public V getProperty() {
       return property;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.vertexId);
        dataOutput.writeInt(this.vertexLabel);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.vertexId = dataInput.readInt();
        this.vertexLabel = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Vertex vertex = (Vertex) o;

        if (vertexId != vertex.vertexId) return false;
        return vertexLabel == vertex.vertexLabel;

    }

    @Override
    public int hashCode() {
        int result = vertexId;
        result = 31 * result + vertexLabel;
        return result;
    }

    @Override
    public String toString() {
        return "Vertex{" +
                "vertexId=" + vertexId +
                ",vertexLabel=" + vertexLabel +
                '}';
    }
}
