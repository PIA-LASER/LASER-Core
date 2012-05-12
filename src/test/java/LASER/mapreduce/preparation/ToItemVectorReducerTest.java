package LASER.mapreduce.preparation;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import static org.mockito.Mockito.*;

public class ToItemVectorReducerTest {

    Reducer.Context context;
    ArrayList<VectorWritable> vectors;
    VectorWritable output;
    VarIntWritable itemID;

    @Before
    public void setup() {
        //input
        vectors = new ArrayList<VectorWritable>();
        VectorWritable v1 = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 1));
        VectorWritable v2 = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 1));
        v1.get().setQuick(1, 0.5);
        v2.get().setQuick(2,1.0);
        vectors.add(v1);
        vectors.add(v2);

        itemID = new VarIntWritable(5);

        //output
        output = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 2));
        output.get().setQuick(1,0.5);
        output.get().setQuick(2,1.0);

        //context
        context = mock(Reducer.Context.class);
    }

    @Test
    public void testToItemVectorReducer() throws IOException, InterruptedException{
        ToItemVectorReducer reducer = new ToItemVectorReducer();

        reducer.reduce(itemID, vectors, context);

        verify(context, times(1)).write(itemID, output);
    }
}
