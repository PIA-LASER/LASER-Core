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
import LASER.mapreduce.validation.RedditCategoryLinkMapper;
import LASER.mapreduce.validation.RedditCategoryLinkReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

    public static void run(App app, HashMap<String, String> args) throws Exception {
        String[] params = new String[args.size()];

        params[0] = args.get("nameNode");
        params[1] = args.get("jobTracker");
        params[2] = args.get("simType");
        params[3] = args.get("numSim");
        params[4] = args.get("redisHost");
        params[5] = args.get("debug");
        params[6] = args.get("outputBoth");
        params[7] = args.get("io.sort.mb");
        params[8] = args.get("io.sort.factor");

        main(params);
    }

    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Logger logger = LoggerFactory.getLogger(App.class);

        //Configuration
        Configuration conf = new Configuration();
        conf.set("similarity", args[2]);
        conf.set("mapred.job.tracker", args[1]);
        conf.set("fs.default.name", args[0]);
        conf.set("maxSimilarities", args[3]);
        conf.set("redisHost", args[4]);
        conf.set("debug", args[5]);
        conf.set("io.sort.mb", args[7]);
        conf.set("io.sort.factor", args[8]);
        conf.set("outputBoth", args[6]);
        HDFSUtil.cleanupTemporaryPath(conf);
        HDFSUtil.cleanupDebugPath(conf);
        HDFSUtil.cleanupOutputPath(conf);

        String outputPrefix = conf.get("maxSimilarities") + "_" + conf.get("similarity") + "_";

        Path inputPath = HDFSUtil.getInputPath();
        Path outputPath = HDFSUtil.getOutputPath();
        Path userVectors = new Path(HDFSUtil.getTemporaryPath(), outputPrefix+"userVectors");
        Path itemVectors = new Path(HDFSUtil.getTemporaryPath(), outputPrefix+"itemVectors");
        Path normedVectors = new Path(HDFSUtil.getTemporaryPath(), outputPrefix+"normedVectors");
        Path partialDots = new Path(HDFSUtil.getTemporaryPath(), outputPrefix+"partialDots");
        Path similarityMatrix = new Path(HDFSUtil.getTemporaryPath(), outputPrefix+"similarityMatrix");
        Path recommendPrepUsers = new Path(HDFSUtil.getTemporaryPath(), outputPrefix+"recommendPrepUsers");
        Path recommendPrepItems = new Path(HDFSUtil.getTemporaryPath(), outputPrefix+"recommendPrepItems");
        Path recommendCombinedMapResults = new Path(recommendPrepItems + "," + recommendPrepUsers);
        Path recommendPrepPairs = new Path(HDFSUtil.getTemporaryPath(), outputPrefix+"recommendPrepPairs");
        Path debugOutputPath = new Path(HDFSUtil.getOutputPath(), outputPrefix+"recommendations");
        Path stats = new Path(HDFSUtil.getOutputPath(), outputPrefix+"stats");
        Path catItemMap = new Path(HDFSUtil.getOutputPath(), outputPrefix+"itemCatMapping");

        //Map categories to items

        boolean success;

        Job mapItemMapping = HadoopUtil.buildJob(
                inputPath,
                catItemMap,
                TextInputFormat.class,
                TextOutputFormat.class,
                RedditCategoryLinkMapper.class,
                LongWritable.class,
                LongWritable.class,
                RedditCategoryLinkReducer.class,
                Text.class,
                Text.class,
                conf
        );

        success = mapItemMapping.waitForCompletion(true);

        if (!success) {
            logger.error("mapItemMapping failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);
            return -1;
        }

        conf.set("mapred.reduce.tasks","2");

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

        success = userVectorJob.waitForCompletion(true);

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

        //bis hier alles fein =)

        if (!success) {
            logger.error("VectorNormsJob failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);
            return -1;
        }

        conf.set("mapred.reduce.tasks","15");

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
            logger.info("Clecaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);

            return -1;
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
            return -1;
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
            return -1;
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
            return -1;
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
            return -1;
        }


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
            return -1;
        }

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
            return -1;
        }


        Job statsJob = HadoopUtil.buildJob(
                userVectors,
                stats,
                ToItemVectorMapper.class,
                VarIntWritable.class,
                VectorWritable.class,
                LinkStatsReducer.class,
                VarIntWritable.class,
                VectorWritable.class,
                conf);

        success = statsJob.waitForCompletion(true);

        if (!success) {
            logger.error("Calculation statistics failed. Aborting.");
            logger.info("Cleaning up temporary files.");
            HDFSUtil.cleanupTemporaryPath(conf);
            return -1;
        }

        return 0;
    }
}
