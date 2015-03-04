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

    public static Job createCountSizeJob(Path aggregatedOut, Path sizeCountTmp) throws IOException {
        Path input = Path.mergePaths(aggregatedOut, new Path("/part-00000"));
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


    public static void main(String[] args) throws Exception {
        Path unigramInput = new Path(args[0]);
        Path bigramInput = new Path(args[1]);
        Path aggregatedTmp = new Path(args[2]);
        Path sizeCountTmp = new Path(args[3]);
        //Path unigramMessageTmp = new Path(args[4]);
        //Path output = new Path(args[5]);
        //int numReducers = Integer.valueOf(args[2]);
        JobControl control = new JobControl("Phrase Finding");

        // Create job objects
        Job aggregateJob = createAggregateJob(unigramInput, bigramInput, aggregatedTmp);
        Job countSizeJob = createCountSizeJob(aggregatedTmp, sizeCountTmp);

        // Specify dependencies
        countSizeJob.addDependingJob(aggregateJob);

        // Add jobs to controller
        control.addJob(aggregateJob);
        control.addJob(countSizeJob);

        // Run all jobs
        control.run();

        //aggregateUnigram(unigramInput, unigramProcessedTmp);
        //aggregateBigram(bigramInput, bigramProcessedTmp);

        /*JobConf conf = new JobConf(Aggregate.class);

        conf.setJobName("Aggr");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(NB_train_hadoop.TokenizerMapper.class);
        conf.setCombinerClass(NB_train_hadoop.IntSumReducer.class);
        conf.setReducerClass(NB_train_hadoop.IntSumReducer.class);

        FileInputFormat.setInputPaths(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, outputPath);

        conf.setNumReduceTasks(numReducers);

        JobClient.runJob(conf);*/
    }
}
