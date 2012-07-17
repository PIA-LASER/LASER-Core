package LASER.mapreduce.recommendation;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.Iterator;

public class RecommendationReducer extends Reducer<VarIntWritable, PrefAndSimilarityColumnWritable, String[], Double> {

    @Override
    public void reduce(VarIntWritable userId, Iterable<PrefAndSimilarityColumnWritable> values, Context context)
           throws IOException, InterruptedException {

        Vector numerators = null;
        Vector denominators = null;

        Iterator<PrefAndSimilarityColumnWritable> iter = values.iterator();

        while (iter.hasNext()) {
            PrefAndSimilarityColumnWritable pascw = iter.next();
            Vector similarities = pascw.getSimilarityColumn();
            float preference = pascw.getPrefValue();

            if(numerators == null) {
                numerators = similarities.times(preference);
            } else {
                numerators = numerators.plus(similarities.times(preference));
            }

            if(denominators == null) {
                denominators = similarities;
            } else {
                denominators = denominators.plus(similarities);
            }
        }

        Iterator<Vector.Element> itemsIter = numerators.iterateNonZero();
        while(itemsIter.hasNext()) {
            Vector.Element item = itemsIter.next();

            int itemId = item.index();
            double recommendation = numerators.get(itemId); // denominators.get(itemId);

            System.out.println(recommendation);

            if(!Double.isNaN(recommendation)){
                String[] userItemPair = new String[]{new Integer(userId.get()).toString(),new Integer(itemId).toString()};
                context.write(userItemPair, recommendation);
            }
        }
    }
}
