package LASER.mapreduce.similarity;

import LASER.Utils.LaserUtils;
import LASER.mapreduce.similarity.measures.Similarity;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors;

import java.io.IOException;

/*
for each user:

Input: UserID: (ItemID1: pref1, ItemID2: pref2, ItemID3 partialDot)
Output: ItemID1 : (ItemID2: partialDot, ItemID3: partialDot, ...)
*/

public class PartialDotMapper extends Mapper<VarIntWritable, VectorWritable, VarIntWritable, VectorWritable> {

    private static Similarity similarity;

    @Override
    public void setup(Context context) {
        similarity = LaserUtils.getSimilarity(context);
    }

    @Override
    public void map(VarIntWritable userId, VectorWritable preferences, Context context) throws IOException, InterruptedException {

        Vector.Element[] vectorElements = Vectors.toArray(preferences);

        int numElements = vectorElements.length;

        for (int leftIndex = 0; leftIndex < numElements; leftIndex++) {
            Vector.Element leftElement = vectorElements[leftIndex];
            VectorWritable vw = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 100));

            for (int rightIndex = leftIndex; rightIndex < numElements; rightIndex++) {
                Vector.Element rightElement = vectorElements[rightIndex];

                double partialDot = similarity.dot(leftElement.get(), rightElement.get());

                vw.get().set(rightElement.index(), partialDot);
            }

            context.write(new VarIntWritable(leftElement.index()), vw);
        }
    }
}
