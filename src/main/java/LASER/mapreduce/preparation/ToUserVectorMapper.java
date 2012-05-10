package LASER.mapreduce.preparation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

import java.io.IOException;
import java.util.regex.Pattern;

public class ToUserVectorMapper extends Mapper<LongWritable, Text, VarIntWritable, VarLongWritable> {

    private static final String SEPARATOR = "[,\t]";
    private static Pattern pattern = Pattern.compile(SEPARATOR);

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
        String[] tokens = pattern.split(line.toString());

        int userId = Integer.parseInt(tokens[0]);
        long itemId = Long.parseLong(tokens[1]);
        float pref = Float.parseFloat(tokens[2]);

        context.write(new VarIntWritable(userId), new EntityPrefWritable(itemId, pref));
    }

}
