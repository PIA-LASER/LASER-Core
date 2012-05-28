package LASER;

import LASER.mapreduce.recommendation.RecommendationReducer;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.AggregateAndRecommendReducer;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecommenderTest {

    List<PrefAndSimilarityColumnWritable> values;
    VarLongWritable userID;
    Reducer.Context context;

    @Before
    public void setup() {
        userID = new VarLongWritable(10);

        PrefAndSimilarityColumnWritable a = new PrefAndSimilarityColumnWritable();
        Vector simVecA = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        simVecA.setQuick(4, 0.6932713389396667);
        simVecA.setQuick(3, 0.7235321998596191);
        simVecA.setQuick(2, 0.9935297966003418);
        simVecA.setQuick(1, Double.NaN);
        a.set(9.0f, simVecA);

        PrefAndSimilarityColumnWritable b = new PrefAndSimilarityColumnWritable();
        Vector simVecB = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        simVecB.setQuick(4, 0.6911254525184631);
        simVecB.setQuick(3, 0.6848383545875549);
        simVecB.setQuick(2, Double.NaN);
        simVecB.setQuick(1, 0.9935297966003418);
        b.set(7.0f, simVecB);

        PrefAndSimilarityColumnWritable c = new PrefAndSimilarityColumnWritable();
        Vector simVecC = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        simVecC.setQuick(4, 0.7468054890632629);
        simVecC.setQuick(3, Double.NaN);
        simVecC.setQuick(2, 0.6848383545875549);
        simVecC.setQuick(1, 0.7235321998596191);
        c.set(5.0f, simVecC);

        PrefAndSimilarityColumnWritable d = new PrefAndSimilarityColumnWritable();
        Vector simVecD = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        simVecD.setQuick(4, Double.NaN);
        simVecD.setQuick(3, 0.7468054890632629);
        simVecD.setQuick(2, 0.6911254525184631);
        simVecD.setQuick(1, 0.6932713389396667);
        d.set(3.0f, simVecD);

        values = new ArrayList<PrefAndSimilarityColumnWritable>();

        values.add(a);
        values.add(b);
        values.add(c);
        values.add(d);

        context = mock(Reducer.Context.class);
    }


    @Test
    public void recommenderTest() throws IOException, InterruptedException{
        RecommendationReducer reducer = new RecommendationReducer();

        reducer.reduce(userID, values, context);
    }
}
