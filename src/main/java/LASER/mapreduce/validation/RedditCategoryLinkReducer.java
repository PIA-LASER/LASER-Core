package LASER.mapreduce.validation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarLongWritable;

import java.io.IOException;
import java.util.HashSet;

public class RedditCategoryLinkReducer extends Reducer<LongWritable, LongWritable, Text, Text> {

    @Override
    public void reduce(LongWritable item, Iterable<LongWritable> categories, Context context) throws IOException, InterruptedException{
        HashSet<Long> cats = new HashSet<Long>();

        for(LongWritable cat : categories) {
            cats.add(new Long(cat.get()));
        }

        for(Long cat : cats) {
            context.write(new Text(new Long(item.get()).toString()), new Text(cat.toString()));
        }
    }
}
