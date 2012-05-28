package LASER.mapreduce.recommendation;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.List;

public class PrepareRecommendationMapper extends Mapper<VarIntWritable, VectorAndPrefsWritable, VarIntWritable, PrefAndSimilarityColumnWritable> {

    @Override
    public void map(VarIntWritable itemId, VectorAndPrefsWritable values, Context context)
            throws IOException, InterruptedException {

        Vector similarities = values.getVector();
        List<Long> userIds = values.getUserIDs();
        List<Float> prefs = values.getValues();

        PrefAndSimilarityColumnWritable pascw = new PrefAndSimilarityColumnWritable();
        VarIntWritable user = new VarIntWritable();

        for(int userIndex=0; userIndex < userIds.size();userIndex++) {
            pascw.set(prefs.get(userIndex), similarities);
            user.set(userIds.get(userIndex).intValue());
            context.write(user, pascw);
        }
    }
}
