package LASER.mapreduce.validation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.regex.Pattern;

public class RedditCategoryLinkMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    private static final String SEPARATOR = "[,]";
    private static Pattern pattern = Pattern.compile(SEPARATOR);

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String[] tokens = pattern.split(line.toString().trim());

        Long item = Long.parseLong(tokens[2]); //linkid
        Long sub = Long.parseLong(tokens[1]); //category

        context.write(new LongWritable(item), new LongWritable(sub));
    }
}
