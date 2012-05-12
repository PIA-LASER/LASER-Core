package LASER.mapreduce.preparation;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class ToUserVectorMapperTest {

    private Text inputLine;
    private VectorWritable output;
    private VarIntWritable userID;
    private int itemID;
    private float pref;
    private Mapper.Context context;


    @Before
    public void setup() {
        inputLine = new Text("1,123456,0.9");

        userID = new VarIntWritable(1);
        itemID = 123456;
        pref = 0.9f;

        output = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 1), true);

        output.get().setQuick(itemID, pref);

        context = mock(Mapper.Context.class);
    }

    @Test
    public void testPartialUserPreferenceVector() throws IOException, InterruptedException{
        ToUserVectorMapper mapper = new ToUserVectorMapper();

        mapper.map(null, inputLine, context);

        verify(context, times(1)).write(userID, output);
    }
}
