import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * Created by vijay on 3/3/15.
 */
public class run {

    public static void aggregate(Path unigramInput, Path bigramInput, Path aggregatedOut) throws IOException{
        JobConf conf = new JobConf(Aggregate.class);
        conf.setJobName("Aggregate");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Aggregate.AggregateMapper.class);
        conf.setReducerClass(Aggregate.AggregateReducer.class);

        FileInputFormat.setInputPaths(conf, unigramInput, bigramInput);
        FileOutputFormat.setOutputPath(conf, aggregatedOut);

        JobClient.runJob(conf);
    }


    public static void main(String[] args) throws Exception {
        Path unigramInput = new Path(args[0]);
        Path bigramInput = new Path(args[1]);
        Path aggregatedTmp = new Path(args[2]);
        Path sizeCountTmp = new Path(args[3]);
        //Path unigramMessageTmp = new Path(args[4]);
        //Path output = new Path(args[5]);
        //int numReducers = Integer.valueOf(args[2]);

        aggregate(unigramInput, bigramInput, aggregatedTmp);
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
