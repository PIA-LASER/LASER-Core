package LASER.mapreduce.preparation;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

public class ToItemVectorReducer extends Reducer<VarIntWritable, VectorWritable, VarIntWritable, VectorWritable> {

    @Override
    protected void reduce(VarIntWritable itemKey, Iterable<VectorWritable> userPrefs, Context context)
            throws IOException, InterruptedException{
        VectorWritable vector = VectorWritable.merge(userPrefs.iterator());

        vector.setWritesLaxPrecision(true);
        context.write(itemKey, vector);
    }
}
