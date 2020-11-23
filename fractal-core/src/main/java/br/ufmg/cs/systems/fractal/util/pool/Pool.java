package br.ufmg.cs.systems.fractal.util.pool;

import br.ufmg.cs.systems.fractal.util.collection.ReclaimableObjCollection;

public interface Pool<O> {
   O createObject();
   void reclaimObject(O object);
   void reclaimObjects(ReclaimableObjCollection<O> objects);
}
