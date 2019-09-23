package br.ufmg.cs.systems.fractal.gmlib.fsm;

import br.ufmg.cs.systems.fractal.aggregation.PatternAggregationAwareValue;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.pattern.VertexPositionEquivalences;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.ClearSetConsumer;
import br.ufmg.cs.systems.fractal.util.IntWriterConsumer;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntCollectionAddConsumer;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.Arrays;

public class DomainSupport implements Writable, Externalizable, PatternAggregationAwareValue {
    private static final ThreadLocal<ClearSetConsumer> clearSetConsumer =
            new ThreadLocal<ClearSetConsumer>() {
                @Override
                protected ClearSetConsumer initialValue() {
                    return new ClearSetConsumer();
                }
            };

    private HashIntSet[] domainSets;
    private HashIntSet domainsReachedSupport;
    private boolean enoughSupport;
    private int support;
    private long currentSupport;
    private int numberOfDomains;
    private IntWriterConsumer intWriterConsumer;
    private IntCollectionAddConsumer intAdderConsumer;
    private boolean setFromSubgraph;
    private Subgraph subgraph;

    public DomainSupport() {
        this.numberOfDomains = 0;
        this.domainsReachedSupport = HashIntSets.newMutableSet();
        this.enoughSupport = false;
        this.setFromSubgraph = false;
        this.currentSupport = 1;
    }

    public DomainSupport(int support) {
        this();
        this.support = support;
    }

    public void setFromSubgraph(Subgraph subgraph) {
        setFromSubgraph = true;
        this.subgraph = subgraph;
    }

    public int getSupport() {
        return support;
    }

    public void setSupport(int support) {
       if (this.support != support) {
         this.support = support;
         this.clear();
       }
    }

    private boolean hasDomainReachedSupport(int i) {
        return i < numberOfDomains && (enoughSupport || domainsReachedSupport.contains(i));
    }

    private void insertDomainsAsFrequent(int i) {
        if (enoughSupport) {
            return;
        }

        domainsReachedSupport.add(i);

        if (domainSets[i] != null) {
            domainSets[i].clear();
        }

        if (domainsReachedSupport.size() == numberOfDomains) {
            this.clear();
            enoughSupport = true;
        }
    }

    private HashIntSet getDomainSet(int i) {
        HashIntSet domainSet = domainSets[i];

        if (domainSet == null) {
            domainSet = domainSets[i] = HashIntSets.newMutableSet();
        }

        return domainSet;
    }

    public void ensureCanStoreNDomains(int size) {
        if (domainSets == null) {
            this.domainSets = new HashIntSet[size];
        } else if (domainSets.length < size) {
            domainSets = Arrays.copyOf(domainSets, size);
        }
    }

    public void clear() {
        clearDomains();

        setFromSubgraph = false;
        subgraph = null;
    }

    private void clearDomains() {
        if (domainSets != null) {
            for (int i = 0; i < domainSets.length; ++i) {
                IntSet domain = domainSets[i];

                if (domain != null) {
                    domain.clear();
                }
            }
        }

        domainsReachedSupport.clear();

        enoughSupport = false;
    }

    public boolean hasEnoughSupport() {
        return enoughSupport;
    }

    private void convertFromSubgraphToNormal() {
        numberOfDomains = subgraph.getNumVertices();
        ensureCanStoreNDomains(numberOfDomains);

        clearDomains();

        IntArrayList vertexMap = subgraph.getVertices();

        for (int i = 0; i < numberOfDomains; i++) {
            if (hasDomainReachedSupport(i)) {
                continue;
            }

            HashIntSet domain = getDomainSet(i);

            domain.add(vertexMap.getUnchecked(i));

            if (domain.size() >= support) {
                insertDomainsAsFrequent(i);
            }
        }

        setFromSubgraph = false;
        subgraph = null;
    }
    
    public void writeExternal(ObjectOutput objOutput) throws IOException {
       write (objOutput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (setFromSubgraph) {
            convertFromSubgraphToNormal();
        }

        dataOutput.writeInt(support);
        dataOutput.writeLong(currentSupport);
        dataOutput.writeInt(numberOfDomains);

        if (enoughSupport) {
            dataOutput.writeBoolean(true);
        } else {
            dataOutput.writeBoolean(false);

            if (intWriterConsumer == null) {
                intWriterConsumer = new IntWriterConsumer();
            }

            intWriterConsumer.setDataOutput(dataOutput);

            dataOutput.writeInt(domainsReachedSupport.size());
            domainsReachedSupport.forEach(intWriterConsumer);

            for (int i = 0; i < numberOfDomains; ++i) {
                if (domainsReachedSupport.contains(i)) {
                    continue;
                }

                dataOutput.writeInt(domainSets[i].size());
                domainSets[i].forEach(intWriterConsumer);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
       readFields (objInput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.clear();

        support = dataInput.readInt();
        currentSupport = dataInput.readLong();
        numberOfDomains = dataInput.readInt();

        if (dataInput.readBoolean()) {
            enoughSupport = true;
        } else {
            enoughSupport = false;

            ensureCanStoreNDomains(numberOfDomains);

            int numDomainsReachedSupport = dataInput.readInt();
            for (int i = 0; i < numDomainsReachedSupport; ++i) {
                domainsReachedSupport.add(dataInput.readInt());
            }

            for (int i = 0; i < numberOfDomains; ++i) {
                if (domainsReachedSupport.contains(i)) {
                    continue;
                }

                int domainSize = dataInput.readInt();

                HashIntSet domainSet = getDomainSet(i);

                for (int j = 0; j < domainSize; ++j) {
                    domainSet.add(dataInput.readInt());
                }
            }

        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GspanPatternSupportAggregation{" +
                "domainsReachedSupport=" + domainsReachedSupport +
                ", enoughSupport=" + enoughSupport +
                ", support=" + support +
                ", currentSupport=" + currentSupport +
                ", numberOfDomains=" + numberOfDomains +
                ", domainSets=" + domainSets +
                ", intWriterConsumer=" + intWriterConsumer +
                ", intAdderConsumer=" + intAdderConsumer +
                ", setFromSubgraph=" + setFromSubgraph +
                ", subgraph=" + subgraph);

        if (domainSets != null) {
            for (int i = 0; i < numberOfDomains; i++) {
                HashIntSet domainSet = domainSets[i];

                if (domainSet == null) {
                    continue;
                }

                sb.append(",domain[" + i + "]=" + domainSets[i]);
            }
        }

        sb.append('}');

        return sb.toString();
    }

    public String toStringResume() {
        StringBuilder sb = new StringBuilder();
        sb.append("GspanPatternSupportAggregation{" +
                "domainsReachedSupport=" + domainsReachedSupport +
                ", enoughSupport=" + enoughSupport +
                ", support=" + support +
                ", numberOfDomains=" + numberOfDomains);

        if (domainSets != null) {
            for (int i = 0; i < numberOfDomains; i++) {
                HashIntSet domainSet = domainSets[i];

                if (domainSet == null) {
                    continue;
                }

                sb.append(",domain[" + i + "]=" + domainSet.size());
            }
        }

        sb.append('}');

        return sb.toString();
    }

    public void aggregate(final HashIntSet[] domains) {
        if (enoughSupport)
            return;

        for (int i = 0; i < numberOfDomains; ++i) {
            aggregate(domains[i], i);

            if (enoughSupport) {
                break;
            }
        }
    }

    private void aggregate(HashIntSet otherDomain, int i) {
        if (otherDomain == null) {
            return;
        }

        if (hasDomainReachedSupport(i)) {
            return;
        }

        HashIntSet domain = getDomainSet(i);

        if (domain == otherDomain) {
            return;
        }

        addAll(domain, otherDomain);

        if (domain.size() >= support) {
            insertDomainsAsFrequent(i);
        }
    }

    private void addAll(IntCollection destination, IntCollection source) {
        if (intAdderConsumer == null) {
            intAdderConsumer = new IntCollectionAddConsumer();
        }

        intAdderConsumer.setCollection(destination);

        source.forEach(intAdderConsumer);
    }

    private void SubgraphAggregate(Subgraph subgraph) {
        int numVertices = subgraph.getNumVertices();

        if (numVertices != numberOfDomains) {
            throw new RuntimeException("Expected " + numberOfDomains + " vertices, got " + numVertices);
        }

        IntArrayList vertices = subgraph.getVertices();

        for (int i = 0; i < numVertices; ++i) {
            if (hasDomainReachedSupport(i)) {
                continue;
            }

            HashIntSet domain = getDomainSet(i);

            domain.add(vertices.getUnchecked(i));

            if (domain.size() >= support) {
                insertDomainsAsFrequent(i);
            }
        }
    }

    public void aggregate(DomainSupport other) {
        if (this == other)
            return;

        // current support
        this.currentSupport += other.currentSupport;

        // If we already have support, do nothing
        if (this.enoughSupport) {
            return;
        }

        // If other simply references an subgraph, do special quick aggregate
        if (other.setFromSubgraph) {
            SubgraphAggregate(other.subgraph);
            return;
        }

        // If the other has enough support, make us have enough support too
        if (other.enoughSupport) {
            this.clear();
            this.enoughSupport = true;
            return;
        }

        if (getNumberOfDomains() != other.getNumberOfDomains()) {
            throw new RuntimeException("Incompatible aggregation of DomainSupports: # of domains differs");
        }

        addAll(domainsReachedSupport, other.domainsReachedSupport);

        if (domainsReachedSupport.size() == numberOfDomains) {
            this.clear();
            this.enoughSupport = true;
            return;
        }

        ClearSetConsumer clearConsumer = clearSetConsumer.get();
        clearConsumer.setSupportMatrix(this.domainSets);
        other.domainsReachedSupport.forEach(clearConsumer);

        aggregate(other.domainSets);
    }

    public int getNumberOfDomains() {
        return numberOfDomains;
    }

    @Override
    public void handleConversionFromQuickToCanonical(Pattern quickPattern, Pattern canonicalPattern) {
        if (hasEnoughSupport()) {
            return;
        }

        // Taking into account automorphisms of the quick pattern, merge
        // equivalent positions
        VertexPositionEquivalences vertexPositionEquivalences = quickPattern.getVertexPositionEquivalences();

        if (vertexPositionEquivalences.getNumVertices() != numberOfDomains) {
            throw new RuntimeException("Mismatch between # number domains and size of autovertexset");
        }

        for (int i = 0; i < numberOfDomains; ++i) {
            IntSet equivalencesToDomainI = vertexPositionEquivalences.getEquivalences(i);
            IntCursor cursor = equivalencesToDomainI.cursor();
            HashIntSet currentDomainSet = getDomainSet(i);

            while (cursor.moveNext()) {
                int equivalentDomainIndex = cursor.elem();

                if (hasDomainReachedSupport(i)) {
                    insertDomainsAsFrequent(equivalentDomainIndex);
                } else {
                    aggregate(currentDomainSet, equivalentDomainIndex);
                }
            }
        }

        // Rearrange to match canonical pattern structure
        IntIntMap canonicalLabeling = canonicalPattern.getCanonicalLabeling();

        HashIntSet[] oldDomainSets = Arrays.copyOf(domainSets, numberOfDomains);
        HashIntSet oldDomainsReachedSupport = HashIntSets.newMutableSet(domainsReachedSupport);
        domainsReachedSupport.clear();

        for (int i = 0; i < numberOfDomains; ++i) {
            // Equivalent position in the canonical pattern to position i in the quick pattern
            int minDomainIndex = canonicalLabeling.get(i);

            domainSets[minDomainIndex] = oldDomainSets[i];

            if (oldDomainsReachedSupport.contains(i)) {
                domainsReachedSupport.add(minDomainIndex);
            }
        }
    }
}
