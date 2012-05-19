package LASER.mapreduce.preparation.similarity;

import LASER.mapreduce.similarity.ItemSimilarityReducer;
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

import static org.junit.Assert.assertTrue;

public class ItemSimilarityReducerTest {

    Configuration conf;
    Reducer.Context context;
    ArrayList<VectorWritable> input;
    int itemID;

    @Before
    public void setup(){
        itemID = 1;

        Vector inputVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
        inputVector.setQuick(2,4.0);

        input = new ArrayList<VectorWritable>();
        input.add(new VectorWritable(inputVector));
    }

    @Test
    public void testCosineSimilarityCalculation() throws IOException, InterruptedException{
        conf = new Configuration();
        conf.set("similarity","CosineSimilarity");

        context = mock(Reducer.Context.class);
        stub(context.getConfiguration()).toReturn(conf);


        ItemSimilarityReducer reducer = new ItemSimilarityReducer();

        reducer.setup(context);

        reducer.reduce(new VarIntWritable(itemID), input, context);

        verify(context, times(1)).write(new VarIntWritable(itemID), input.get(0));
    }
}
