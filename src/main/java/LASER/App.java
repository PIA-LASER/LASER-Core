package LASER;

import LASER.Utils.HDFSUtil;
import LASER.Utils.HadoopUtil;
import LASER.mapreduce.preparation.ToItemVectorMapper;
import LASER.mapreduce.preparation.ToItemVectorReducer;
import LASER.mapreduce.preparation.ToUserVectorMapper;
import LASER.mapreduce.preparation.ToUserVectorReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

public class App 
{
    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
    {
        HDFSUtil.cleanupTemporaryPath();

        Path inputPath = HDFSUtil.getInputPath();
        Path outputPath = HDFSUtil.getOutputPath();

        Path userVectors = new Path(HDFSUtil.getTemporaryPath(), "userVectors");
        Path itemVectors = new Path(HDFSUtil.getTemporaryPath(), "itemVectors");

        Job userVectorJob = HadoopUtil.buildJob(
                inputPath,
                userVectors,
                TextInputFormat.class,
                SequenceFileOutputFormat.class,
                ToUserVectorMapper.class,
                VarIntWritable.class,
                VectorWritable.class,
                ToUserVectorReducer.class,
                VarIntWritable.class,
                VectorWritable.class,
                new Configuration());

        boolean success = userVectorJob.waitForCompletion(true);

        Job itemVectorJob = HadoopUtil.buildJob(
                userVectors,
                itemVectors,
                ToItemVectorMapper.class,
                VarIntWritable.class,
                VectorWritable.class,
                ToItemVectorReducer.class,
                VarIntWritable.class,
                VectorWritable.class,
                new Configuration());

        success = itemVectorJob.waitForCompletion(true);
    }
}
