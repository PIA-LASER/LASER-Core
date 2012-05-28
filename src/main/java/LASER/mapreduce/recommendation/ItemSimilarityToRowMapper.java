package LASER.mapreduce.recommendation;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

public class ItemSimilarityToRowMapper extends Mapper<VarIntWritable, VectorWritable, VarIntWritable, VectorOrPrefWritable> {

    @Override
    public void map(VarIntWritable itemId, VectorWritable similarities, Context context) throws IOException, InterruptedException{
        VectorOrPrefWritable vopw = new VectorOrPrefWritable();

        //no self similarity
        similarities.get().set(itemId.get(), Double.NaN);

        vopw.set(similarities.get());

        context.write(itemId, vopw);
    }
}
