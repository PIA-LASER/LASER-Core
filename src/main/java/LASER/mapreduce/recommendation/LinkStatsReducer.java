package LASER.mapreduce.recommendation;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class LinkStatsReducer extends Reducer<IntWritable,LongWritable, Text, Text> {

    @Override
    public void reduce(IntWritable itemId, Iterable<LongWritable> timestamps, Context context) throws IOException, InterruptedException{
        int count = 0;
        long maxAge = System.currentTimeMillis() / 1000L;

        for (LongWritable timestamp : timestamps) {
            count++;

            if(maxAge > timestamp.get())
                maxAge = timestamp.get();
        }

        String output = itemId + "," + count + "," + maxAge;

        maxAge = maxAge - System.currentTimeMillis() / 1000L;
        double timeAge = maxAge / 3600.0d;

        Jedis redis = new Jedis(context.getConfiguration().get("redisHost"));

        double score = (double)count / (Math.pow((double)timeAge,1.8) + 1);

        redis.zadd("urls.popular", new Double(score)  ,new Integer(itemId.get()).toString());

        redis.disconnect();

        context.write(new Text(), new Text(output));
    }
}
