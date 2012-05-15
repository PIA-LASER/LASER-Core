package LASER.mapreduce.preparation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import static org.mockito.Mockito.*;

public class ToItemVectorMapperTest {

    private int userId;
    private Vector prefs;

    private Vector outputVector;
    private Mapper.Context context;

    @Before
    public void setup(){
        //input
        userId = 1;
        prefs = new RandomAccessSparseVector(Integer.MAX_VALUE, 3);
        prefs.setQuick(5,1.0);

        //context
        context =  mock(Mapper.Context.class);

        //output
        outputVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
        outputVector.setQuick(1,1.0);
    }

    @Test
    public void testToItemVectorMapper() throws IOException, InterruptedException{
        ToItemVectorMapper mapper = new ToItemVectorMapper();

        mapper.map(new VarIntWritable(userId), new VectorWritable(prefs), context);

        verify(context, times(1)).write(new VarIntWritable(5), new VectorWritable(outputVector, true));
    }

    @Test
    public void testSimilarityMeasureInstantiation() throws InterruptedException, IOException{
        Configuration conf = new Configuration();
        conf.set("similarity","CosineSimilarity");

        Mapper.Context mockedContext = mock(Mapper.Context.class);
        stub(mockedContext.getConfiguration()).toReturn(conf);

        ToItemVectorMapper mapper = new ToItemVectorMapper();
        mapper.setup(mockedContext);
    }
}
