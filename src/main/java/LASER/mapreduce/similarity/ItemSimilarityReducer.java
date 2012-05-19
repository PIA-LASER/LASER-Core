package LASER.mapreduce.similarity;

import LASER.Utils.LaserUtils;
import LASER.mapreduce.similarity.measures.Similarity;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Iterator;

public class ItemSimilarityReducer extends Reducer<VarIntWritable, VectorWritable, VarIntWritable, VectorWritable> {

    Similarity similarity;

    @Override
    public void setup(Context context) {
        similarity = LaserUtils.getSimilarity(context);
    }

    @Override
    public void reduce(VarIntWritable key, Iterable<VectorWritable> dotSums, Context context) throws IOException, InterruptedException{
        for(VectorWritable vw : dotSums){
            Iterator<Vector.Element> iter = vw.get().iterateNonZero();
            while(iter.hasNext()) {
                Vector.Element elem = iter.next();

                double sim = similarity.similarity(elem.get(),0,0,0);
                Vector simPair = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);

                simPair.setQuick(elem.index(), sim);

                context.write(key, new VectorWritable(simPair));
            }
        }
    }
}
