package LASER.mapreduce.preparation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.regex.Pattern;

public class ToUserVectorMapper extends Mapper<LongWritable, Text, VarIntWritable, VectorWritable> {

    private static final String SEPARATOR = "[,]";
    private static Pattern pattern = Pattern.compile(SEPARATOR);

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
        String[] tokens = pattern.split(line.toString().trim());

        int userId = Integer.parseInt(tokens[0]);
        int itemId = Integer.parseInt(tokens[2]);
        float pref = Float.parseFloat(tokens[2]);

        VectorWritable vw = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 1), true);

        vw.get().setQuick(itemId, pref);

        context.write(new VarIntWritable(userId), vw);
    }

}
