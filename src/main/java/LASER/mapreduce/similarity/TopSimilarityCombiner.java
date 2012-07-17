package LASER.mapreduce.similarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors;

import java.io.IOException;

public class TopSimilarityCombiner extends Reducer<VarIntWritable, VectorWritable, VarIntWritable, VectorWritable> {

    private int numberOfSimilarities = 20;

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();

        numberOfSimilarities = new Integer(conf.get("maxSimilarities"));

        if(numberOfSimilarities < 1) {
            throw new IllegalArgumentException("Number of used item similarities to small");
        }
    }

    @Override
    public void reduce(VarIntWritable itemId, Iterable<VectorWritable> similarities, Context context)
            throws IOException, InterruptedException{
        Vector simVector = Vectors.merge(similarities);
        Vector simVectorReduced = Vectors.topKElements(numberOfSimilarities, simVector);

        context.write(itemId, new VectorWritable(simVectorReduced));
    }
}
