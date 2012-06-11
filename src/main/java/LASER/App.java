package LASER;

import LASER.Utils.HDFSUtil;
import LASER.Utils.HadoopUtil;
import LASER.io.RedisOutputFormat;
import LASER.mapreduce.preparation.ToItemVectorMapper;
import LASER.mapreduce.preparation.ToItemVectorReducer;
import LASER.mapreduce.preparation.ToUserVectorMapper;
import LASER.mapreduce.preparation.ToUserVectorReducer;
import LASER.mapreduce.recommendation.*;
import LASER.mapreduce.similarity.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.HashMap;

public class App extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new App(), args);
    }

    private HashMap<String, String> parseArgs(String[] args) {
        HashMap<String, String> params = new HashMap<String, String>();

        /*
        params.put("nameNode", args[0]);
        params.put("jobTracker", args[1]);
        params.put("simType", args[2]);
        params.put("numSim", args[3]);
        params.put("redisHost", args[4]);
         */
        params.put("nameNode", "hdfs://localhost:54310/");
        params.put("jobTracker", "localhost:54311");
        params.put("simType", "CosineSimilarity");
        params.put("numSim", "100");
        params.put("redisHost", "localhost");
        params.put("debug", "true");
        params.put("outputTypeBoth", "false");

        return params;
    }

    @Override
    public int run(String[] strings) throws IOException, InterruptedException, ClassNotFoundException {
        Logger logger = LoggerFactory.getLogger(App.class);

        HashMap<String, String> args = parseArgs(strings);

        //Configuration
        Configuration conf = new Configuration();
        conf.set("similarity", args.get("simType"));
        conf.set("mapred.job.tracker", args.get("jobTracker"));
        conf.set("fs.default.name", args.get("nameNode"));
        conf.set("maxSimilarities", args.get("numSim"));
        conf.set("redisHost", args.get("redisHost"));
        conf.set("debug", args.get("debug"));
        conf.set("io.sort.mb", "200");
        conf.set("io.sort.factor", "20");

        HDFSUtil.cleanupTemporaryPath(conf);
        HDFSUtil.cleanupDebugPath(conf);
        HDFSUtil.cleanupOutputPath(conf);

        Path inputPath = HDFSUtil.getInputPath();
        Path outputPath = HDFSUtil.getOutputPath();
        Path userVectors = new Path(HDFSUtil.getTemporaryPath(), "userVectors");
        Path itemVectors = new Path(HDFSUtil.getTemporaryPath(), "itemVectors");
        Path normedVectors = new Path(HDFSUtil.getTemporaryPath(), "normedVectors");
        Path partialDots = new Path(HDFSUtil.getTemporaryPath(), "partialDots");
        Path similarityMatrix = new Path(HDFSUtil.getTemporaryPath(), "similarityMatrix");
        Path recommendPrepUsers = new Path(HDFSUtil.getTemporaryPath(), "recommendPrepUsers");
        Path recommendPrepItems = new Path(HDFSUtil.getTemporaryPath(), "recommendPrepItems");
        Path recommendCombinedMapResults = new Path(recommendPrepItems + "," + recommendPrepUsers);
        Path recommendPrepPairs = new Path(HDFSUtil.getTemporaryPath(), "recommendPrepPairs");
        Path debugOutputPath = new Path(HDFSUtil.getDebugPath(), "recommendations");

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
                conf);

        boolean success = userVectorJob.waitForCompletion(true);

        if (!success) {
            logger.error("UserVectorJob failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);
            return -1;
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
                conf);

        success = itemVectorJob.waitForCompletion(true);

        if (!success) {
            logger.error("ItemVectorJob failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);
            return -1;
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
                conf
        );

        normsJob.setCombinerClass(MergeVectorsCombiner.class);

        success = normsJob.waitForCompletion(true);

        if (!success) {
            logger.error("VectorNormsJob failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);
            return -1;
        }

        Job partialDotJob = HadoopUtil.buildJob(
                normedVectors,
                partialDots,
                PartialDotMapper.class,
                VarIntWritable.class,
                VectorWritable.class,
                ItemSimilarityReducer.class,
                VarIntWritable.class,
                VectorWritable.class,
                conf);

        partialDotJob.setCombinerClass(PartialDotSumCombiner.class);

        success = partialDotJob.waitForCompletion(true);

        if (!success) {
            logger.error("PartialDotJob failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);

            throw new IllegalStateException();
        }

        Job symSimilarityMatrixJob = HadoopUtil.buildJob(
                partialDots,
                similarityMatrix,
                SimilarityMatrixMapper.class,
                VarIntWritable.class,
                VectorWritable.class,
                MergeVectorsCombiner.class,
                VarIntWritable.class,
                VectorWritable.class,
                conf);


        symSimilarityMatrixJob.setCombinerClass(TopSimilarityCombiner.class);

        success = symSimilarityMatrixJob.waitForCompletion(true);

        if (!success) {
            logger.error("SymSimilarityMatrixJob failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);
            throw new IllegalStateException();
        }

        Job getItemRowsJob = HadoopUtil.buildJob(
                similarityMatrix,
                recommendPrepItems,
                ItemSimilarityToRowMapper.class,
                VarIntWritable.class,
                VectorOrPrefWritable.class,
                Reducer.class,
                VarIntWritable.class,
                VectorOrPrefWritable.class,
                conf
        );

        success = getItemRowsJob.waitForCompletion(true);

        if (!success) {
            logger.error("getItemRowsJob failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);
            throw new IllegalStateException();
        }

        Job getUserPrefsJob = HadoopUtil.buildJob(
                userVectors,
                recommendPrepUsers,
                UserPreferenceToRowMapper.class,
                VarIntWritable.class,
                VectorOrPrefWritable.class,
                Reducer.class,
                VarIntWritable.class,
                VectorOrPrefWritable.class,
                conf
        );

        success = getUserPrefsJob.waitForCompletion(true);

        if (!success) {
            logger.error("getUserPrefsJob failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);
            throw new IllegalStateException();
        }

        Job pairItemsAndPrefs = HadoopUtil.buildJob(
                recommendCombinedMapResults,
                recommendPrepPairs,
                Mapper.class,
                VarIntWritable.class,
                VectorOrPrefWritable.class,
                RecommendationPreperationReducer.class,
                VarIntWritable.class,
                VectorAndPrefsWritable.class,
                conf
        );

        success = pairItemsAndPrefs.waitForCompletion(true);

        if (!success) {
            logger.error("pairItemsAndPrefs failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);
            throw new IllegalStateException();
        }

        if (args.get("debug") == "false" || args.get("outputTypeBoth") == "true") {

            Job recommendItems = HadoopUtil.buildJob(
                    recommendPrepPairs,
                    outputPath,
                    SequenceFileInputFormat.class,
                    RedisOutputFormat.class,
                    PrepareRecommendationMapper.class,
                    VarIntWritable.class,
                    PrefAndSimilarityColumnWritable.class,
                    RecommendationReducer.class,
                    Text.class,
                    Text.class,
                    conf
            );

            success = recommendItems.waitForCompletion(true);

            if (!success) {
                logger.error("Recommendation failed. Aborting.");
                logger.info("Cleaning up temporary files.");
                HDFSUtil.cleanupTemporaryPath(conf);
                throw new IllegalStateException();
            }
        }

        if (args.get("debug") == "true" || args.get("outputTypeBoth") == "true") {
            Job debugRecommendationJob = HadoopUtil.buildJob(
                    recommendPrepPairs,
                    debugOutputPath,
                    SequenceFileInputFormat.class,
                    TextOutputFormat.class,
                    PrepareRecommendationMapper.class,
                    VarIntWritable.class,
                    PrefAndSimilarityColumnWritable.class,
                    DebugOutputReducer.class,
                    Text.class,
                    Text.class,
                    conf
            );

            success = debugRecommendationJob.waitForCompletion(true);

            if (!success) {
                logger.error("Debug recommendation output failed. Aborting.");
                logger.info("Cleaning up temporary files.");
                HDFSUtil.cleanupTemporaryPath(conf);
                throw new IllegalStateException();
            }
        }

        return 0;
    }
}
