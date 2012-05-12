package LASER.mapreduce.preparation;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.*;

import java.io.IOException;
import java.util.Iterator;


public class ToItemVectorMapper extends Mapper<VarIntWritable, VectorWritable, VarIntWritable, VectorWritable> {

    @Override
    protected void map(VarIntWritable key, VectorWritable prefs, org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException{
        int userId = key.get();

        VectorWritable preferencesVector = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 1));
        preferencesVector.setWritesLaxPrecision(true);

        Iterator<Vector.Element> iterator = prefs.get().iterateNonZero();

        while(iterator.hasNext()) {
            Vector.Element elem = iterator.next();
            preferencesVector.get().setQuick(userId, elem.get());
            context.write(new VarIntWritable(elem.index()), preferencesVector);
        }
    }
}
