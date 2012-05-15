package LASER;

import LASER.Utils.HDFSUtil;
import LASER.Utils.HadoopUtil;
import LASER.mapreduce.preparation.ToItemVectorMapper;
import LASER.mapreduce.preparation.ToItemVectorReducer;
import LASER.mapreduce.preparation.ToUserVectorMapper;
import LASER.mapreduce.preparation.ToUserVectorReducer;
import LASER.mapreduce.similarity.VectorNormMapper;
import LASER.mapreduce.similarity.VectorNormMergeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class App 
{
    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
    {
        Logger logger = LoggerFactory.getLogger(App.class);

        HDFSUtil.cleanupTemporaryPath();

        Path inputPath = HDFSUtil.getInputPath();
        Path outputPath = HDFSUtil.getOutputPath();

        Path userVectors = new Path(HDFSUtil.getTemporaryPath(), "userVectors");
        Path itemVectors = new Path(HDFSUtil.getTemporaryPath(), "itemVectors");
        Path normedVectors = new Path(HDFSUtil.getTemporaryPath(), "normedVectors");

        //Configuration

        Configuration config = new Configuration();
        config.set("similarity","CosineSimilarity");

        //Map input to user vectors
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
                config);

        boolean success = userVectorJob.waitForCompletion(true);

        if(!success) {
            logger.error("UserVectorJob failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath();
            return;
        }

        //Map userVectors to itemVectors
        Job itemVectorJob = HadoopUtil.buildJob(
                userVectors,
                itemVectors,
                ToItemVectorMapper.class,
                VarIntWritable.class,
                VectorWritable.class,
                ToItemVectorReducer.class,
                VarIntWritable.class,
                VectorWritable.class,
                config);

        success = itemVectorJob.waitForCompletion(true);

        if(!success) {
            logger.error("ItemVectorJob failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath();
            return;
        }

        Job normsJob = HadoopUtil.buildJob(
                itemVectors,
                normedVectors,
                VectorNormMapper.class,
                VarIntWritable.class,
                VectorWritable.class,
                VectorNormMergeReducer.class,
                VarIntWritable.class,
                VectorWritable.class,
                config
        );

        success = normsJob.waitForCompletion(true);

        if(!success) {
            logger.error("VectorNormsJob failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath();
            return;
        }
        //build similarity matrix

        //recommendation
    }
}
