package br.ufmg.cs.systems.fractal.util.collection;

import com.koloboke.collect.set.hash.HashObjSet;
import com.koloboke.collect.set.hash.HashObjSets;

import java.io.*;
import java.util.Collection;
import java.util.Iterator;

// TODO: make this class a java.util.Collection
public class ObjSet<T> implements Externalizable {
   transient private HashObjSet<T> underlying;

   public ObjSet() {
      this.underlying = HashObjSets.newMutableSet();
   }

   public void add(T elem) {
      this.underlying.add(elem);
   }

   public void addAll(Collection<T> collection) {
      for (T elem : collection) this.underlying.add(elem);
   }

   public void addAll(ObjSet<T> other) {
      addAll(other.underlying);
   }

   public boolean contains(T elem) {
      return this.underlying.contains(elem);
   }

   public int size() {
      return underlying.size();
   }

   public Iterator<T> iterator() {
      return underlying.iterator();
   }

   @Override
   public void writeExternal(ObjectOutput objectOutput) throws IOException {
      objectOutput.writeInt(underlying.size());
      for (T elem : underlying) {
         objectOutput.writeObject(elem);
      }
   }

   @Override
   public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
      int size = objectInput.readInt();
      underlying.ensureCapacity(size);
      for (int i = 0; i < size; ++i) {
         underlying.add((T) objectInput.readObject());
      }
   }

   @Override
   public String toString() {
      return underlying.toString();
   }
}
