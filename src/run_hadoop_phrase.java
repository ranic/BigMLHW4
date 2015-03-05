import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by vijay on 3/3/15.
 */
public class run_hadoop_phrase {

    private static Path filenameFromPath(Path p) {
        return p;
    }

    public static Job createAggregateJob(Path unigramInput, Path bigramInput, Path aggregatedOut) throws IOException{

        Configuration conf = new Configuration();
        Job job = new Job(conf, "Aggregate");
        job.setJarByClass(Aggregate.class);

        job.setMapperClass(Aggregate.AggregateMapper.class);
        job.setReducerClass(Aggregate.AggregateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, unigramInput, bigramInput);
        FileOutputFormat.setOutputPath(job, aggregatedOut);

        return job;
    }

    public static Job createCountSizeJob(Path aggregatedInput, Path sizeCountTmp) throws IOException {
        Path input = filenameFromPath(aggregatedInput);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "CountSize");
        job.setJarByClass(CountSize.class);

        job.setMapperClass(CountSize.CountSizeMapper.class);
        job.setReducerClass(CountSize.CountSizeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, aggregatedInput);
        FileOutputFormat.setOutputPath(job, sizeCountTmp);

        return job;
    }

    public static Job createMessageUnigramJob(Path aggregatedInput, Path messageUnigramRequestsTmp) throws IOException {
        Path aggregatedInputFile = filenameFromPath(aggregatedInput);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "MessageUnigram");
        job.setJarByClass(MessageUnigram.class);

        job.setMapperClass(MessageUnigram.MessageMapper.class);
        job.setReducerClass(MessageUnigram.MessageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, aggregatedInputFile);
        FileOutputFormat.setOutputPath(job, messageUnigramRequestsTmp);

        return job;
    }

    public static Job createBigramCountsJob(Path aggregatedInput, Path messageUnigramInput, Path bigramCountsTmp) throws IOException {
        Path unigramInputFile = filenameFromPath(messageUnigramInput);
        Path aggregatedInputFile = filenameFromPath(aggregatedInput);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "BigramCounts");
        job.setJarByClass(MessageUnigram.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MessageUnigram.IdentityMapper.class);
        job.setReducerClass(MessageUnigram.ConcatenateReducer.class);

        FileInputFormat.setInputPaths(job, aggregatedInputFile, unigramInputFile);
        FileOutputFormat.setOutputPath(job, bigramCountsTmp);

        return job;
    }

    public static Job createComputeJob(Path bigramCountsInput, Path finalOutput, Path sizeCountTmp) throws IOException {
        Path bigramCountsFile = filenameFromPath(bigramCountsInput);
        Path sizeCountFile = filenameFromPath(sizeCountTmp);

        Configuration conf = new Configuration();
        conf.set("sizeCountFile", sizeCountFile.toString() + "/part-r-00000");
        Job job = new Job(conf, "Compute");
        job.setJarByClass(Compute.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Compute.ComputeMapper.class);
        job.setReducerClass(Compute.ComputeReducer.class);

        FileInputFormat.setInputPaths(job, bigramCountsFile);
        FileOutputFormat.setOutputPath(job, finalOutput);


        return job;
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path unigramInput = new Path(args[0]);
        Path bigramInput = new Path(args[1]);
        Path aggregatedTmp = new Path(args[2]);
        Path sizeCountTmp = new Path(args[3]);
        Path messageUnigramRequestsTmp = new Path(args[4] + "/requests");
        Path bigramCountsTmp = new Path(args[4] + "/bigramCounts");
        Path finalOutput = new Path(args[5]);

        // Create job objects
        Job aggregateJob = createAggregateJob(unigramInput, bigramInput, aggregatedTmp);
        Job countSizeJob = createCountSizeJob(aggregatedTmp, sizeCountTmp);
        Job messageUnigramJob = createMessageUnigramJob(aggregatedTmp, messageUnigramRequestsTmp);
        Job bigramCountsJob = createBigramCountsJob(aggregatedTmp, messageUnigramRequestsTmp, bigramCountsTmp);
        Job computeJob = createComputeJob(bigramCountsTmp, finalOutput, sizeCountTmp);

        countSizeJob.setNumReduceTasks(1);

        // Run jobs in order
        aggregateJob.waitForCompletion(true);
        messageUnigramJob.waitForCompletion(true);
        bigramCountsJob.waitForCompletion(true);
        countSizeJob.waitForCompletion(true);
        computeJob.waitForCompletion(true);
    }
}
