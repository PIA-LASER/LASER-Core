package LASER.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class RedisOutputFormat<K, V> extends OutputFormat {

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        final String redisHost = conf.get("redisHost");

        return new RecordWriter() {
            Jedis con = new Jedis(redisHost);

            @Override
            public void write(Object o, Object o1) throws IOException, InterruptedException {
                String[] userItemPair = (String[]) o;
                Double pref = (Double) o1;
                con.zadd( "users." + userItemPair[0], pref , userItemPair[1]);
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                con.disconnect();
            }
        };
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {}

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new OutputCommitter() {
            @Override
            public void setupJob(JobContext jobContext) throws IOException {}

            @Override
            public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {}

            @Override
            public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException { return false; }

            @Override
            public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {}

            @Override
            public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {}
        };
    }
}