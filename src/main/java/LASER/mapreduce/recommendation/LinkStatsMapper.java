package LASER.mapreduce.recommendation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class LinkStatsMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

    private static final String SEPARATOR = "[,]";
    private static Pattern pattern = Pattern.compile(SEPARATOR);

    @Override
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
        String[] tokens = pattern.split(line.toString().trim());

        int itemId = Integer.parseInt(tokens[2]);
        long timestamp = Long.parseLong(tokens[3]);

        context.write(new IntWritable(itemId), new LongWritable(timestamp));
    }
}
