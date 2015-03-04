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


    public static void aggregateUnigram(Path unigramInput, Path unigramAggregated) throws IOException{
        JobConf conf = new JobConf(Aggregate.class);
        conf.setJobName("Aggregate Unigram");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Aggregate.UnigramMapper.class);
        conf.setReducerClass(Aggregate.UnigramReducer.class);

        FileInputFormat.setInputPaths(conf, unigramInput);
        FileOutputFormat.setOutputPath(conf, unigramAggregated);

        JobClient.runJob(conf);
    }

    public static void aggregateBigram(Path bigramInput, Path bigramAggregated) throws IOException{
        JobConf conf = new JobConf(Aggregate.class);
        conf.setJobName("Aggregate Bigram");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Aggregate.BigramMapper.class);
        conf.setReducerClass(Aggregate.BigramReducer.class);

        FileInputFormat.setInputPaths(conf, bigramInput);
        FileOutputFormat.setOutputPath(conf, bigramAggregated);

        JobClient.runJob(conf);
    }




    public static void main(String[] args) throws Exception {
        Path unigramInput = new Path(args[0]);
        Path bigramInput = new Path(args[1]);
        Path unigramProcessedTmp = new Path(args[2] + "/unigram_processed");
        Path bigramProcessedTmp = new Path(args[2] + "/bigram_processed");
        Path sizeCountTmp = new Path(args[3]);
        //Path unigramMessageTmp = new Path(args[4]);
        //Path output = new Path(args[5]);
        //int numReducers = Integer.valueOf(args[2]);

        aggregateUnigram(unigramInput, unigramProcessedTmp);
        aggregateBigram(bigramInput, bigramProcessedTmp);

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
