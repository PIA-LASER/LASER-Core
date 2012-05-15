package LASER.mapreduce.preparation.similarity;

import LASER.mapreduce.similarity.VectorNormMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class VectorNormMapperTest {

    int userId;
    Vector userPreferences;
    Vector normalizedUserPreferences;
    HashMap<Integer, Vector> outputPrefs;
    Mapper.Context context;
    Configuration conf;

    @Before
    public void setup() {
        userId = 1;

        userPreferences = new RandomAccessSparseVector(Integer.MAX_VALUE, 3);

        userPreferences.setQuick(5, 1.0);
        userPreferences.setQuick(6, 4.0);
        userPreferences.setQuick(7, 9.0);

        normalizedUserPreferences = userPreferences.normalize();

        outputPrefs = new HashMap<Integer, Vector>();

        Iterator<Vector.Element> iter = normalizedUserPreferences.iterateNonZero();

        while (iter.hasNext()) {
            Vector.Element elem = iter.next();

            Vector v = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
            v.setQuick(userId, elem.get());

            outputPrefs.put(elem.index(), v);
        }

        context = mock(Mapper.Context.class);

        conf = new Configuration();
        conf.set("similarityName", "CosineSimilarity");

        stub(context.getConfiguration()).toReturn(conf);
    }


    @Test
    public void testNormalizingVectors() throws IOException, InterruptedException{
        VectorNormMapper mapper = new VectorNormMapper();

        mapper.setup(context);
        mapper.map(new VarIntWritable(userId), new VectorWritable(userPreferences ,true),context);

        Iterator<Integer> iter = outputPrefs.keySet().iterator();

        while (iter.hasNext()) {
            Integer itemId = iter.next();

            verify(context,times(1)).write(new VarIntWritable(itemId.intValue()), new VectorWritable(outputPrefs.get(itemId)));
        }
    }
}
