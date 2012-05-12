package LASER.mapreduce.preparation;

import LASER.Utils.LaserUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.mockito.Mockito.*;

public class ToUserVectorReducerTest {

    int userID;
    ArrayList<VectorWritable> inputPreferences;
    VectorWritable outputPreferences;
    Reducer.Context context;

    @Before
    public void setup() {
        //input
        int userID = 1;

        inputPreferences = new ArrayList<VectorWritable>();

        Vector v1 = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
        Vector v2 = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);

        v1.setQuick(5,1.0);
        v2.setQuick(6,-1.0);

        inputPreferences.add(new VectorWritable(v1, true));
        inputPreferences.add(new VectorWritable(v2, true));


        outputPreferences = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 2));
        outputPreferences.get().setQuick(5,1.0);
        outputPreferences.get().setQuick(6,-1.0);

        context = mock(Reducer.Context.class);
    }

    @Test
    public void userVectorWithIntegerIdItem() throws IOException, InterruptedException{
        ToUserVectorReducer reducer = new ToUserVectorReducer();

        reducer.reduce(new VarIntWritable(userID), inputPreferences, context);

        verify(context, times(1)).write(new VarIntWritable(userID), outputPreferences);
    }
/*
    public void userVectorWithLongIdItem() {

    }
*/
}
