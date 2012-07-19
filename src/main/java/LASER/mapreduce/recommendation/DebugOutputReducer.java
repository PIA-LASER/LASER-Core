package LASER.mapreduce.recommendation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Iterator;

public class DebugOutputReducer extends Reducer<VarIntWritable, PrefAndSimilarityColumnWritable, Text, Text> {
    @Override
    public void reduce(VarIntWritable userId, Iterable<PrefAndSimilarityColumnWritable> values, Context context)
            throws IOException, InterruptedException {

        Vector numerators = null;
        Vector denominators = null;

        Iterator<PrefAndSimilarityColumnWritable> iter = values.iterator();

        while (iter.hasNext()) {
            PrefAndSimilarityColumnWritable pascw = iter.next();
            Vector similarities = pascw.getSimilarityColumn();

            Iterator<Vector.Element> simIter = similarities.iterateNonZero();

            float preference = pascw.getPrefValue();

            if (numerators == null) {
                numerators = similarities.times(preference);
            } else {
                numerators = numerators.plus(similarities.times(preference));
            }

            if (denominators == null) {
                denominators = similarities;
            } else {
                denominators = denominators.plus(similarities);
            }
        }

        Iterator<Vector.Element> itemsIter = numerators.iterateNonZero();
        while (itemsIter.hasNext()) {
            Vector.Element item = itemsIter.next();

            int itemId = item.index();
            double recommendation = numerators.get(itemId);  // denominators.get(itemId);
	
            if (!Double.isNaN(recommendation)) {
                String user = new Integer(userId.get()).toString();
                context.write(new Text(""), new Text(user + "," + itemId + "," + recommendation));
            }
        }
    }
}
