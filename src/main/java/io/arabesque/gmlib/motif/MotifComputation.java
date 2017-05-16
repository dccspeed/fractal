package io.arabesque.gmlib.motif;

import io.arabesque.aggregation.reductions.LongSumReduction;
import io.arabesque.computation.VertexInducedComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.VertexInducedEmbedding;
import org.apache.hadoop.io.LongWritable;

public class MotifComputation
      extends VertexInducedComputation<VertexInducedEmbedding> {
    public static final String AGG_MOTIFS = "motifs";
    private static final String MAXSIZE = "arabesque.motif.maxsize";
    private static final int MAXSIZE_DEFAULT = 4;

    private static LongWritable reusableLongWritableUnit = new LongWritable(1);

    private int maxsize;

    @Override
    public void init(Configuration config) {
       super.init(config);
       maxsize = getConfig().getInteger(MAXSIZE, MAXSIZE_DEFAULT);
    }

    @Override
    public void initAggregations(Configuration config) {
        super.initAggregations(config);

        config.registerAggregation(AGG_MOTIFS, config.getPatternClass(),
              LongWritable.class, true, new LongSumReduction());
    }

    @Override
    public boolean shouldExpand(VertexInducedEmbedding embedding) {
        return embedding.getNumVertices() < maxsize;
    }

    @Override
    public void process(VertexInducedEmbedding embedding) {
        if (embedding.getNumWords() == maxsize) {
            map(AGG_MOTIFS, embedding.getPattern(), reusableLongWritableUnit);
            output(embedding);
        }
    }
}
