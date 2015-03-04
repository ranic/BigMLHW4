import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

import java.io.IOException;

/**
 * Created by vijay on 3/3/15.
 */
public class run {

    public static Job createAggregateJob(Path unigramInput, Path bigramInput, Path aggregatedOut) throws IOException{
        JobConf conf = new JobConf(Aggregate.class);
        conf.setJobName("Aggregate");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Aggregate.AggregateMapper.class);
        conf.setReducerClass(Aggregate.AggregateReducer.class);

        FileInputFormat.setInputPaths(conf, unigramInput, bigramInput);
        FileOutputFormat.setOutputPath(conf, aggregatedOut);

        Job j = new Job(conf);
        return j;
    }

    public static Job createCountSizeJob(Path aggregatedInput, Path sizeCountTmp) throws IOException {
        Path input = Path.mergePaths(aggregatedInput, new Path("/part-00000"));
        JobConf conf = new JobConf();

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(CountSize.CountSizeMapper.class);
        conf.setReducerClass(CountSize.CountSizeReducer.class);

        FileInputFormat.setInputPaths(conf, input);
        FileOutputFormat.setOutputPath(conf, sizeCountTmp);

        Job j = new Job(conf);
        return j;
    }

    public static Job createMessageUnigramJob(Path aggregatedInput, Path messageUnigramRequestsTmp) throws IOException {
        Path aggregatedInputFile = Path.mergePaths(aggregatedInput, new Path("/part-00000"));
        JobConf conf = new JobConf();

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MessageUnigram.MessageMapper.class);
        conf.setReducerClass(MessageUnigram.MessageReducer.class);

        FileInputFormat.setInputPaths(conf, aggregatedInputFile);
        FileOutputFormat.setOutputPath(conf, messageUnigramRequestsTmp);

        Job j = new Job(conf);
        return j;
    }

    public static Job createBigramCountsJob(Path aggregatedInput, Path messageUnigramInput, Path bigramCountsTmp) throws IOException {
        Path unigramInputFile = Path.mergePaths(messageUnigramInput, new Path("/part-00000"));
        Path aggregatedInputFile = Path.mergePaths(aggregatedInput, new Path("/part-00000"));

        JobConf conf = new JobConf();

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MessageUnigram.IdentityMapper.class);
        conf.setReducerClass(MessageUnigram.ConcatenateReducer.class);

        FileInputFormat.setInputPaths(conf, aggregatedInputFile, unigramInputFile);
        FileOutputFormat.setOutputPath(conf, bigramCountsTmp);

        Job j = new Job(conf);
        return j;
    }

    public static Job createComputeJob(Path bigramCountsInput, Path finalOutput, Path sizeCountTmp) throws IOException {
        Path bigramCountsFile = Path.mergePaths(bigramCountsInput, new Path("/part-00000"));
        JobConf conf = new JobConf();

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Compute.ComputeMapper.class);
        conf.setReducerClass(Compute.ComputeReducer.class);

        FileInputFormat.setInputPaths(conf, bigramCountsFile);
        FileOutputFormat.setOutputPath(conf, finalOutput);

        conf.set("sizeCountFile", sizeCountTmp.toString() + "/part-00000");

        Job j = new Job(conf);
        return j;
    }


    public static void main(String[] args) throws Exception {
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

        // Specify dependencies
        //countSizeJob.addDependingJob(aggregateJob);
        //messageUnigramJob.addDependingJob(aggregateJob);
        //bigramCountsJob.addDependingJob(messageUnigramJob);

        // Add jobs to controller
        JobControl control = new JobControl("Phrase Finding");
        //control.addJob(aggregateJob);
        //control.addJob(countSizeJob);
        //control.addJob(messageUnigramJob);
        //control.addJob(bigramCountsJob);
        control.addJob(computeJob);

        // Run all jobs
        control.run();
    }


}
