package LASER.mapreduce.similarity;

import LASER.Utils.LaserUtils;
import LASER.mapreduce.similarity.measures.Similarity;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Iterator;

public class VectorNormMapper extends Mapper<VarIntWritable, VectorWritable, VarIntWritable, VectorWritable> {

    private static Similarity similarity;

    @Override
    public void setup(Context context){
        similarity = LaserUtils.getSimilarity(context);
    }

    @Override
    public void map(VarIntWritable itemId, VectorWritable vw, Context context) throws IOException, InterruptedException{
        Vector vector = similarity.normalize(vw.get());

        Iterator<Vector.Element> iter = vector.iterateNonZero();

        while (iter.hasNext()) {
            Vector.Element elem = iter.next();

            Vector userItemVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);

            userItemVector.setQuick(itemId.get(), elem.get());

            context.write(new VarIntWritable(elem.index()), new VectorWritable(userItemVector, true));
        }
    }
}
