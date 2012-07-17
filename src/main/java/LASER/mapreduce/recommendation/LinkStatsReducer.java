package LASER.mapreduce.recommendation;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class LinkStatsReducer extends Reducer<VarIntWritable,VectorWritable, Text, Text> {

    @Override
    public void reduce(VarIntWritable itemId, Iterable<VectorWritable> prefs, Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();

        Jedis con = new Jedis(conf.get("redisHost"));

        long totalCount = 0;

        for(VectorWritable vw : prefs) {
            totalCount += vw.get().getNumNondefaultElements();
        }

        con.set("urls." + itemId.get() + ".popularity", new Long(totalCount).toString());

        con.disconnect();
    }
}
