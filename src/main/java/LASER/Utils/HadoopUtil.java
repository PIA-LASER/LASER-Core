package LASER.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class HadoopUtil {

    public static Job buildJob(Path inputPath,
                               Path outputPath,
                               Class<? extends Mapper> mapper,
                               Class<? extends Writable> mapKey,
                               Class<? extends Writable> mapValue,
                               Class<? extends Reducer> reducer,
                               Class<? extends Writable> reducerKey,
                               Class<? extends Writable> reducerValue,
                               Configuration configuration) throws IOException {
        return buildJob(inputPath,outputPath, SequenceFileInputFormat.class , SequenceFileOutputFormat.class ,mapper,mapKey,mapValue,reducer,reducerKey,reducerValue,configuration);
    }

    public static Job buildJob(Path inputPath,
                               Path outputPath,
                               Class<? extends InputFormat> inputFormat,
                               Class<? extends OutputFormat> outputFormat,
                               Class<? extends Mapper> mapper,
                               Class<? extends Writable> mapKey,
                               Class<? extends Writable> mapValue,
                               Class<? extends Reducer> reducer,
                               Class<? extends Writable> reducerKey,
                               Class<? extends Writable> reducerValue,
                               Configuration configuration) throws IOException {

        Job job = new Job(new Configuration(configuration));

        if (reducer.equals(Reducer.class)) {
            if (mapper.equals(Mapper.class)) {
                throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
            }
            job.setJarByClass(mapper);
        } else {
            job.setJarByClass(reducer);
        }

        Configuration config = job.getConfiguration();

        job.setInputFormatClass(inputFormat);
        config.set("mapred.input.dir", inputPath.toString());

        job.setMapperClass(mapper);
        job.setMapOutputKeyClass(mapKey);
        job.setMapOutputValueClass(mapValue);

        job.setReducerClass(reducer);
        job.setOutputKeyClass(reducerKey);
        job.setOutputValueClass(reducerValue);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputFormatClass(outputFormat);
        config.set("mapred.output.dir", outputPath.toString());

        return job;
    }
}