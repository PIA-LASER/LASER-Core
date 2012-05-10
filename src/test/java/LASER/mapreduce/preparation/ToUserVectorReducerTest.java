package LASER.mapreduce.preparation;

import LASER.Utils.LaserUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.*;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.mockito.Mockito.*;

public class ToUserVectorReducerTest {

    @Test
    public void userVectorWithIntegerIdItem() throws IOException, InterruptedException{
        ToUserVectorReducer reducer = new ToUserVectorReducer();

        Reducer.Context context = mock(Context.class);

        VarIntWritable userId = new VarIntWritable(1);

        ArrayList<VarLongWritable> prefEntities = new ArrayList<VarLongWritable>();

        prefEntities.add(new EntityPrefWritable(1, 0.8f));

        reducer.reduce(userId, prefEntities, context);



        //resulting sparse vector

        Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        EntityPrefWritable pref = (EntityPrefWritable) prefEntities.get(0);

        userVector.setQuick(LaserUtils.idToIndex(pref.getID()), pref.getPrefValue());

        verify(context, times(1)).write(new VarIntWritable(1), new VectorWritable(userVector));
    }
/*
    public void userVectorWithLongIdItem() {

    }
*/
}
