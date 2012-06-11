package LASER.mapreduce.preparation.similarity;

import LASER.mapreduce.similarity.MergeVectorsCombiner;
import LASER.mapreduce.similarity.TopSimilarityCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.mockito.Mockito.*;

public class TopSimilarityCombinerTest {
    Reducer.Context context;
    int itemId;
    ArrayList<VectorWritable> inputVectors;
    VectorWritable outputVector;

    @Before
    public void setup() {
        context = mock(Reducer.Context.class);

        Configuration conf = new Configuration();
        conf.set("maxSimilarities", "1");

        stub(context.getConfiguration()).toReturn(conf);

        itemId = 1;

        inputVectors = new ArrayList<VectorWritable>();

        Vector v1 = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
        v1.setQuick(4, 5.0);
        Vector v2 = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
        v1.setQuick(5,1.0);
        Vector v3 = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
        v3.setQuick(6,3.0);

        inputVectors.add(new VectorWritable(v1, true));
        inputVectors.add(new VectorWritable(v2, true));
        inputVectors.add(new VectorWritable(v3, true));

        Vector vOutput = new RandomAccessSparseVector(Integer.MAX_VALUE, 3);
        vOutput.setQuick(4,5.0);
        vOutput.setQuick(5,1.0);
        vOutput.setQuick(6,3.0);

        outputVector = new VectorWritable(vOutput, true);
    }

    @Test
    public void testMergingVectors() throws IOException, InterruptedException{
        TopSimilarityCombiner reducer = new TopSimilarityCombiner();

        reducer.setup(context);

        reducer.reduce(new VarIntWritable(itemId), inputVectors, context);

        //verify(context, times(1)).write(new VarIntWritable(itemId), outputVector);
    }
}
