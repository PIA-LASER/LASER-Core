package LASER.mapreduce.recommendation;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Iterator;

public class UserPreferenceToRowMapper extends Mapper<VarIntWritable, VectorWritable, VarIntWritable, VectorOrPrefWritable> {

    @Override
    public void map(VarIntWritable userId, VectorWritable userPreferences, Context context) throws IOException, InterruptedException{
        VectorOrPrefWritable vopw = new VectorOrPrefWritable();

        Iterator<Vector.Element> iter = userPreferences.get().iterateNonZero();

        while (iter.hasNext()) {
            Vector.Element elem = iter.next();

            vopw.set(userId.get(), (float)elem.get());

            context.write(new VarIntWritable(elem.index()), vopw);
        }
    }
}
