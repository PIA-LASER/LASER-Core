package LASER.mapreduce.recommendation;


import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecommendationPreperationReducer extends Reducer<VarIntWritable, VectorOrPrefWritable, VarIntWritable, VectorAndPrefsWritable> {

    @Override
    public void reduce(VarIntWritable itemId, Iterable<VectorOrPrefWritable> vopws, Context context) throws
            IOException, InterruptedException {

        List<Long> userIDs = new ArrayList<Long>();
        List<Float> preferences = new ArrayList<Float>();
        Vector similarityVector = null;

        Iterator<VectorOrPrefWritable> vopwsIter = vopws.iterator();

        while(vopwsIter.hasNext()) {
            VectorOrPrefWritable vopw = vopwsIter.next();

            if(vopw.getVector() == null) {
                userIDs.add(vopw.getUserID());
                preferences.add(vopw.getValue());
            } else {
                similarityVector = vopw.getVector();
            }
        }
         /*

        System.out.println("Users: ");

        for(int i=0;i<userIDs.size();i++) {
            System.out.println("User: " + userIDs.get(i) + " , Pref: " + preferences.get(i));
        }

        Iterator<Vector.Element> simIter = similarityVector.iterateNonZero();

        while (simIter.hasNext())
        {
            Vector.Element elem = simIter.next();

            System.out.println("(" + elem.index() + "," + elem.get() +")");
        }  */

        //user has preference for item we do not know
        if(similarityVector == null) {
            return;
        }


        VectorAndPrefsWritable vapw = new VectorAndPrefsWritable(similarityVector, userIDs, preferences);

        context.write(itemId, vapw);
    }
}
