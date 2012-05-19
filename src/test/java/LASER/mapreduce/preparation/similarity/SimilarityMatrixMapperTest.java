package LASER.mapreduce.preparation.similarity;

import LASER.mapreduce.similarity.SimilarityMatrixMapper;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class SimilarityMatrixMapperTest {

    VectorWritable input;
    VectorWritable out1;
    VectorWritable out2;
    int itemID;
    int otherItemID;
    Mapper.Context context;

    @Before
    public void setup() {
        itemID = 1;
        otherItemID = 2;
        Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
        Vector o1 = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
        Vector o2 = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);

        input = new VectorWritable(vector);
        out1 = new VectorWritable(o1);
        out2 = new VectorWritable(o2);

        input.get().setQuick(otherItemID, 3.0);

        out1.get().setQuick(otherItemID,3.0);
        out2.get().setQuick(itemID,3.0);

        context = mock(Mapper.Context.class);
    }

    @Test
    public void testBuildingSymmetricSimilarityMatrix() throws IOException, InterruptedException{
        SimilarityMatrixMapper mapper = new SimilarityMatrixMapper();

        mapper.map(new VarIntWritable(itemID), input, context);

        verify(context, times(1)).write(new VarIntWritable(itemID), out1);
        verify(context, times(1)).write(new VarIntWritable(otherItemID), out2);
    }
}
