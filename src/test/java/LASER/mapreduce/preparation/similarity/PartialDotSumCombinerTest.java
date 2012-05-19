package LASER.mapreduce.preparation.similarity;

import LASER.mapreduce.similarity.PartialDotSumCombiner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

public class PartialDotSumCombinerTest {

    int itemID;
    ArrayList<VectorWritable> input;
    VectorWritable output;
    Reducer.Context context;


    @Before
    public void setup() {

        context = mock(Reducer.Context.class);

        Vector v1 = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
        Vector v2 = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);

        VectorWritable vw1 = new VectorWritable(v1);
        VectorWritable vw2 = new VectorWritable(v2);

        vw1.get().setQuick(2,3.0);
        vw2.get().setQuick(2,4.5);

        input = new ArrayList<VectorWritable>();
        input.add(vw1);
        input.add(vw2);

        itemID = 1;

        Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);

        output = new VectorWritable(vector);
        output.get().set(2,7.5);
    }

    @Test
    public void testBuildingSymmetricSimilarityMatrix() throws IOException, InterruptedException{
        PartialDotSumCombiner partialDotSumCombiner = new PartialDotSumCombiner();

        partialDotSumCombiner.reduce(new VarIntWritable(itemID), input, context);

        verify(context, times(1)).write(new VarIntWritable(itemID), output);
    }
}
