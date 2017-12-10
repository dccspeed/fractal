package io.arabesque.embedding;

import io.arabesque.conf.Configuration;
import io.arabesque.computation.Computation;
import io.arabesque.misc.WritableObject;
import io.arabesque.pattern.Pattern;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.AtomicBitSetArray;
import io.arabesque.utils.collection.RoaringBitSet;
import com.koloboke.collect.IntCollection;

import java.io.Externalizable;

public interface Embedding extends WritableObject, Externalizable {
    void init(Configuration configuration);

    IntArrayList getWords();

    IntArrayList getVertices();

    int getNumVertices();

    IntArrayList getEdges();

    int getNumEdges();

    int getNumWords();

    Pattern getPattern();

    int getNumVerticesAddedWithExpansion();

    int getNumEdgesAddedWithExpansion();

    void addWord(int word);

    int getLastWord();
    
    void removeLastWord();

    IntCollection getExtensibleWordIds(Computation computation);
    
    boolean isCanonicalEmbeddingWithWord(int wordId);

    String toOutputString();
    
    void nextExtensionLevel();
    
    void nextExtensionLevel(Embedding other);
    
    void previousExtensionLevel();

    void applyTagFrom(AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos);
    
    void applyTagTo(AtomicBitSetArray vtag, AtomicBitSetArray etag, int pos);
}
