package LASER.mapreduce.similarity;


import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors;

import java.io.IOException;
import java.util.Iterator;

public class MergeVectorsCombiner extends Reducer<VarIntWritable, VectorWritable, VarIntWritable, VectorWritable> {
    @Override
    public void reduce(VarIntWritable key, Iterable<VectorWritable> vectors, Context context) throws IOException, InterruptedException{
        Vector vector = Vectors.merge(vectors);
        context.write(key, new VectorWritable(vector, true));
    }
}
