package br.ufmg.cs.systems.fractal.pattern;

import br.ufmg.cs.systems.fractal.util.collection.IntCollectionAddConsumer;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class VertexPositionEquivalences {
    private IntSet[] equivalences;
    private int numVertices;
    private IntCollectionAddConsumer intAddConsumer = new IntCollectionAddConsumer();

    public VertexPositionEquivalences() {
        this.equivalences = null;
        this.numVertices = -1;
    }

    public VertexPositionEquivalences(VertexPositionEquivalences other) {
        setNumVertices(other.numVertices);
        addAll(other);
    }

    public VertexPositionEquivalences(Map<Integer, Set<Integer>> mapRepresentation) {
        setNumVertices(mapRepresentation.size());

        for (Map.Entry<Integer, Set<Integer>> entry : mapRepresentation.entrySet()) {
            equivalences[entry.getKey()] = HashIntSets.newMutableSet(entry.getValue());
        }
    }

    public void setNumVertices(int numVertices) {
        if (this.numVertices != numVertices) {
            ensureCapacity(numVertices);
            this.numVertices = numVertices;
        }
    }

    public void clear() {
        for (int i = 0; i < equivalences.length; ++i) {
            equivalences[i].clear();
        }

        /*for (int i = 0; i < numVertices; ++i) {
            equivalences[i].add(i);
        }*/
    }


    public void addAll(VertexPositionEquivalences vertexPositionEquivalences) {
        setNumVertices(vertexPositionEquivalences.numVertices);

        for (int i = 0; i < numVertices; ++i) {
            intAddConsumer.setCollection(equivalences[i]);
            vertexPositionEquivalences.equivalences[i].forEach(intAddConsumer);
        }
    }

    public void addEquivalence(int pos1, int pos2) {
        /*if (pos1 == pos2) {
            return;
        }*/

        equivalences[pos1].add(pos2);
        //equivalences[pos2].add(pos1);
    }

    public IntSet getEquivalences(int pos) {
        return equivalences[pos];
    }

    public EdgePositionEquivalences getEdgeEquivalences(PatternEdgeArrayList edges) {
       int numEdges = edges.size();
       IntSet[] equivalences = new IntSet[numEdges];
       
       for (int i = 0; i < numEdges; ++i) {
          equivalences[i] = HashIntSets.newMutableSet();
       }
		
       for (int i = 0; i < numEdges; ++i) {
          PatternEdge edge1 = edges.get(i);
          IntSet src1 = getEquivalences(edge1.getSrcPos());
          IntSet dest1 = getEquivalences(edge1.getDestPos());
          equivalences[i].add(i);
          for (int j = i; j < numEdges; ++j) {
             // are these edges adjacent?
             PatternEdge edge2 = edges.get(j);
             
             IntSet src2 = getEquivalences(edge2.getSrcPos());
             IntSet dest2 = getEquivalences(edge2.getDestPos());

             if ((src1.equals(src2) && dest1.equals(dest2)) ||
                   (src1.equals(dest2) && dest1.equals(src2))) {
                equivalences[i].add(j);
                equivalences[j].add(i);
             }
          }
       }

       return new EdgePositionEquivalences(equivalences);
    }

    public void propagateEquivalences() {
        for (int i = 0; i < numVertices; ++i) {
            IntSet currentVertexEquivalences = equivalences[i];

            if (currentVertexEquivalences != null) {
                IntCursor cursor = currentVertexEquivalences.cursor();

                while (cursor.moveNext()) {
                    int equivalentPosition = cursor.elem();

                    if (equivalentPosition == i) {
                        continue;
                    }

                    intAddConsumer.setCollection(equivalences[equivalentPosition]);
                    currentVertexEquivalences.forEach(intAddConsumer);
                }
            }
        }
    }

    private void ensureCapacity(int n) {
        int numSetsToCreate = n;

        if (equivalences == null) {
            equivalences = new HashIntSet[n];
        }
        else if (equivalences.length < n) {
            numSetsToCreate -= equivalences.length;
            equivalences = Arrays.copyOf(equivalences, n);
        }

        int newSize = equivalences.length;
        int targetI = newSize - numSetsToCreate;

        for (int i = newSize - 1; i >= targetI; --i) {
            equivalences[i] = HashIntSets.newMutableSet();
        }
    }

    public int getNumVertices() {
        return numVertices;
    }

    @Override
    public String toString() {
        return "VertexPositionEquivalences{" +
                "equivalences=" + Arrays.toString(equivalences) +
                ", numVertices=" + numVertices +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VertexPositionEquivalences that = (VertexPositionEquivalences) o;

        if (numVertices != that.numVertices) return false;

        for (int i = 0; i < numVertices; ++i) {
            // Only enter if one is null and the other isn't
            if ((equivalences[i] == null) ^ (that.equivalences[i] == null)) {
                return false;
            }

            if (equivalences[i] != null && !equivalences[i].equals(that.equivalences[i])) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = numVertices;

        for (int i = 0; i < numVertices; ++i) {
            result = 31 * result + (equivalences[i] != null ? equivalences[i].hashCode() : 0);
        }

        return result;
    }

    public void convertBasedOnRelabelling(IntIntMap relabelling) {
        VertexPositionEquivalences oldEquivalences = new VertexPositionEquivalences(this);

        clear();

        IntIntCursor relabellingCursor = relabelling.cursor();

        while (relabellingCursor.moveNext()) {
            int oldPos = relabellingCursor.key();
            IntSet oldEquivalencesForOldPos = oldEquivalences.getEquivalences(oldPos);

            int newPos = relabellingCursor.value();
            IntSet newEquivalencesForNewPos = equivalences[newPos];

            IntCursor oldEquivalencesForOldPosCursor = oldEquivalencesForOldPos.cursor();

            while (oldEquivalencesForOldPosCursor.moveNext()) {
                int oldEquivalentPosition = oldEquivalencesForOldPosCursor.elem();
                int newEquivalentPosition = relabelling.get(oldEquivalentPosition);

                newEquivalencesForNewPos.add(newEquivalentPosition);
            }
        }
    }

    public boolean isEmpty() {
        return numVertices == 0;
    }
}
