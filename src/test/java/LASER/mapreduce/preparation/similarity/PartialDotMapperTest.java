package LASER.mapreduce.preparation.similarity;

import LASER.mapreduce.similarity.PartialDotMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import static org.mockito.Mockito.*;

public class PartialDotMapperTest {

    int userIdA;
    VectorWritable userPrefsA;
    HashMap<VarIntWritable,VectorWritable> dots;

    @Before
    public void setup() {
        userIdA = 1;

        userPrefsA = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 3));

        userPrefsA.get().set(4,1.0);
        userPrefsA.get().set(5,-1.0);
        userPrefsA.get().set(6,1.0);

        dots = new HashMap<VarIntWritable, VectorWritable>();

        Vector.Element[] userAElements = Vectors.toArray(userPrefsA);

        for(int i=0; i<userAElements.length; i++) {
            VectorWritable vw = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 30));

            for(int j=i; j<userAElements.length; j++) {
                double dot = userAElements[i].get() * userAElements[j].get();

                vw.get().setQuick(userAElements[j].index(), dot);
            }

            dots.put(new VarIntWritable(userAElements[i].index()), vw);
        }
    }

    @Test
    public void testPartialDotCalculation() throws IOException, InterruptedException{
        PartialDotMapper mapper = new PartialDotMapper();

        Configuration conf = new Configuration();
        conf.set("similarity","CosineSimilarity");

        Mapper.Context context = mock(Mapper.Context.class);

        stub(context.getConfiguration()).toReturn(conf);

        mapper.setup(context);
        mapper.map(new VarIntWritable(userIdA), userPrefsA, context);

        Set<VarIntWritable> dotKeys = dots.keySet();
        Iterator<VarIntWritable> dotIter = dotKeys.iterator();

        while (dotIter.hasNext()) {
            VarIntWritable key = dotIter.next();
            verify(context,times(1)).write(key,dots.get(key));
        }

    }
}
