package LASER.mapreduce.similarity;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;

import java.io.IOException;
import java.util.Iterator;

public class PartialDotSumCombiner extends Reducer<VarIntWritable, VectorWritable, VarIntWritable, VectorWritable> {

    @Override
    public void reduce(VarIntWritable key, Iterable<VectorWritable> dots, Context context) throws IOException, InterruptedException {
        Iterator<VectorWritable> dotsIter = dots.iterator();

        Vector vector = null;
        for (VectorWritable v : dots) {
            if (vector == null) {
                vector = v.get();
            } else {
                vector.assign(v.get(), Functions.PLUS);
            }
        }
        context.write(key, new VectorWritable(vector, true));
    }
}
