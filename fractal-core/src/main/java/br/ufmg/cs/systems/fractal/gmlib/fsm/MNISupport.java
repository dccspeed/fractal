package br.ufmg.cs.systems.fractal.gmlib.fsm;

import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.pattern.VertexPositionEquivalences;
import br.ufmg.cs.systems.fractal.subgraph.PatternInducedSubgraph;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.IntWriterConsumer;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.ObjArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import br.ufmg.cs.systems.fractal.util.pool.IntSetPool;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSet;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import scala.math.Ordering;

import java.io.*;
import java.util.function.IntPredicate;

public class MNISupport implements Writable, Externalizable {
   private static final Logger LOG = Logger.getLogger(MNISupport.class);

   protected ObjArrayList<IntSet> domainSets;
   protected IntArrayList domainsReachedSupport;
   protected int numDomainsReachedSupport;
   protected boolean enoughSupport;
   protected int supportThreshold;

   protected InsertIntoDomainPredicate insertIntoDomainPredicate;
   protected IntWriterConsumer intWriterConsumer;

   public MNISupport() {
      this.supportThreshold = -1;
      this.enoughSupport = false;
      this.insertIntoDomainPredicate = new InsertIntoDomainPredicate();
      this.intWriterConsumer = new IntWriterConsumer();
   }

   public void reset(int supportThreshold, int numDomains) {
      this.supportThreshold = supportThreshold;
      this.domainSets = new ObjArrayList<>(numDomains);
      this.domainsReachedSupport = IntArrayListPool.instance().createObject();
      this.numDomainsReachedSupport = 0;
      for (int i = 0; i < numDomains; ++i) {
         IntSet domainSet = IntSetPool.instance().createObject();
         this.domainSets.add(domainSet);
         this.domainsReachedSupport.add(0);
      }
      this.enoughSupport = false;
   }

   public boolean enoughSupport() {
      return enoughSupport;
   }

   public void set(int supportThreshold, Subgraph subgraph) {
      if (this.enoughSupport) return;

      if (this.supportThreshold < 0) {
         reset(supportThreshold, subgraph.getNumVertices());
      }

      int numVertices = subgraph.getNumVertices();
      for (int i = 0; i < numVertices; ++i) {
         insertIntoDomain(subgraph.getVertices().getUnchecked(i), i);
         if (this.enoughSupport) break;
      }
   }

   private boolean insertIntoDomain(int u, int i) {
      // overall frequent
      if (this.enoughSupport) return true;

      // specific domain frequent
      if (this.domainsReachedSupport.getUnchecked(i) == 1) return false;

      // domain not frequent
      IntSet domainSet = this.domainSets.getUnchecked(i);
      domainSet.add(u);
      if (domainSet.size() >= this.supportThreshold) {
         this.domainsReachedSupport.setUnchecked(i, 1);
         this.numDomainsReachedSupport++;
         //IntSetPool.instance().reclaimObject(domainSet);
         //this.domainSets.setUnchecked(i, null);
      }

      // reached support
      if (this.numDomainsReachedSupport == this.domainsReachedSupport.size()) {
         this.enoughSupport = true;
         IntArrayListPool.instance().reclaimObject(this.domainsReachedSupport);
         this.domainsReachedSupport = null;
         this.domainSets = null;
         return true;
      }

      return false;

   }

   public void aggregate(MNISupport other) {
      if (other.supportThreshold < 0) {
         return;
      }

      // this has enough support
      if (this.enoughSupport) {
         return;
      }

      // other has enough support -- make this have enough support
      if (other.enoughSupport) {
         this.enoughSupport = true;
         return;
      }

      if (this.supportThreshold < 0) {
         reset(other.supportThreshold, other.domainSets.size());
      }

      // neither has enough support alone, merge domain sets
      int numDomains = this.domainsReachedSupport.size();
      for (int i = 0; i < numDomains; ++i) {
         if (this.domainsReachedSupport.getUnchecked(i) == 1) continue;

         if (other.domainsReachedSupport.getUnchecked(i) == 1) {
            this.domainsReachedSupport.setUnchecked(i, 1);
            this.numDomainsReachedSupport++;
            //if (this.domainSets.getUnchecked(i) != null) {
            //   IntSetPool.instance().reclaimObject(this.domainSets.getUnchecked(i));
            //   this.domainSets.setUnchecked(i, null);
            //}

            // reached support
            if (this.numDomainsReachedSupport == this.domainsReachedSupport.size()) {
               this.enoughSupport = true;
               IntArrayListPool.instance().reclaimObject(this.domainsReachedSupport);
               this.domainsReachedSupport = null;
               this.domainSets = null;
               break;
            }

         } else {
            insertIntoDomainPredicate.set(i);
            other.domainSets.getUnchecked(i).forEachWhile(insertIntoDomainPredicate);
            if (this.enoughSupport) break;
         }
      }


   }

   public void aggregate(Pattern pattern) {
      if (this.enoughSupport) return;
      LOG.info("FinalAggregate " + this + " " + pattern + " " + pattern.getVertexPositionEquivalences());
      VertexPositionEquivalences vertexPositionEquivalences = pattern.getVertexPositionEquivalences();
      for (int i = 0; i < this.domainSets.size(); ++i) {
         if (this.domainsReachedSupport.getUnchecked(i) == 1) continue;
         IntSet vequivalences = vertexPositionEquivalences.getEquivalences(i);
         IntCursor cur = vequivalences.cursor();

         while (cur.moveNext()) {
            int j = cur.elem();

            if (this.domainsReachedSupport.getUnchecked(j) == 1) {
               this.domainsReachedSupport.setUnchecked(i, 1);
               this.numDomainsReachedSupport++;
               //if (this.domainSets.getUnchecked(i) != null) {
               //   IntSetPool.instance().reclaimObject(this.domainSets.getUnchecked(i));
               //   this.domainSets.setUnchecked(i, null);
               //}

               // reached support
               if (this.numDomainsReachedSupport == this.domainsReachedSupport.size()) {
                  this.enoughSupport = true;
                  IntArrayListPool.instance().reclaimObject(this.domainsReachedSupport);
                  this.domainsReachedSupport = null;
                  this.domainSets = null;
                  return;
               }

            } else {
               insertIntoDomainPredicate.set(i);
               this.domainSets.getUnchecked(j).forEachWhile(insertIntoDomainPredicate);
               if (this.enoughSupport) return;
            }
         }
      }
   }

   @Override
   public void write(DataOutput out) throws IOException {
      out.writeInt(this.supportThreshold);
      out.writeBoolean(this.enoughSupport);
      if (this.enoughSupport) return;

      // number of domains
      int numDomains = this.domainSets.size();
      out.writeInt(numDomains);

      // domains
      out.writeInt(this.numDomainsReachedSupport);
      for (int i = 0; i < numDomains; ++i) {
         int domainReachedSupport = this.domainsReachedSupport.getUnchecked(i);
         out.writeInt(domainReachedSupport);
         if (domainReachedSupport == 0) { // write domainset
            IntSet domainSet = this.domainSets.getUnchecked(i);
            out.writeInt(domainSet.size());
            intWriterConsumer.setDataOutput(out);
            domainSet.forEach(intWriterConsumer);
         }
      }
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      this.supportThreshold = in.readInt();
      this.enoughSupport = in.readBoolean();
      if (this.enoughSupport) return;

      // number of domains
      int numDomains = in.readInt();
      reset(this.supportThreshold, numDomains);

      // domains
      this.numDomainsReachedSupport = in.readInt();
      for (int i = 0; i < numDomains; ++i) {
         int domainReachedSupport = in.readInt();
         this.domainsReachedSupport.setUnchecked(i, domainReachedSupport);
         if (domainReachedSupport == 0) {
            IntSet domainSet = this.domainSets.getUnchecked(i);
            int domainSetSize = in.readInt();
            domainSet.ensureCapacity(domainSetSize);
            for (int j = 0; j < domainSetSize; ++j) {
               domainSet.add(in.readInt());
            }
         }
      }
   }

   @Override
   public void writeExternal(ObjectOutput objectOutput) throws IOException {
      write(objectOutput);
   }

   @Override
   public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
      readFields(objectInput);
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      if (domainSets != null) {
         for (int i = 0; i < domainSets.size(); ++i) {
            IntSet domainSet = domainSets.getUnchecked(i);
            sb.append(domainSet != null ? domainSet.size() : -1);
            sb.append(",");
         }
      }
      sb.append("]");
      return "mni{supportThreshold=" + this.supportThreshold +
              ", enoughSupport=" + this.enoughSupport + "}" +
              " " + sb.toString() + " " + domainsReachedSupport +
              " " + numDomainsReachedSupport;
   }

   private class InsertIntoDomainPredicate implements IntPredicate {
      private int domain;

      public void set(int domain) {
         this.domain = domain;
      }

      @Override
      public boolean test(int u) {
         MNISupport.this.insertIntoDomain(u, domain);
         return !MNISupport.this.enoughSupport;
      }
   }
}
