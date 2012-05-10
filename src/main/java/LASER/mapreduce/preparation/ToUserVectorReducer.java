package LASER.mapreduce.preparation;

import LASER.Utils.LaserUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.*;

import java.io.IOException;

public class ToUserVectorReducer extends Reducer<VarIntWritable, VarLongWritable, VarIntWritable, VectorWritable> {

    @Override
    protected void reduce(VarIntWritable userId, Iterable<VarLongWritable> preferences, Context context)
            throws IOException, InterruptedException {

        Vector preferenceVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);

        for (VarLongWritable itemPref : preferences) {
            int index = LaserUtils.idToIndex(itemPref.get());
            float pref = ((EntityPrefWritable) itemPref).getPrefValue();

            preferenceVector.setQuick(index, pref);
        }

        context.write(userId, new VectorWritable(preferenceVector));
    }
}
