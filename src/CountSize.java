import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by vijay on 3/3/15.
 */
public class CountSize {

    public static class CountSizeMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, LongWritable> {

        Text totalFgBigrams = new Text("totalFgBigrams");
        Text totalBgBigrams = new Text("totalBgBigrams");
        Text totalFgUnigrams = new Text("totalFgUnigrams");
        Text uniqueFgBigrams = new Text("uniqueFgBigrams");
        Text uniqueBgBigrams = new Text("uniqueBgBigrams");
        Text uniqueFgUnigrams = new Text("uniqueFgUnigrams");
        LongWritable one = new LongWritable(1);

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text,
                LongWritable> out, Reporter reporter) throws IOException {
            String[] tokens = value.toString().split("\t");
            String phrase = tokens[0];
            String[] counts = tokens[1].split(",");
            boolean isBigram = phrase.contains(" ");

            if (isBigram) {
                int bxy = Integer.valueOf(counts[0].split("=")[1]);
                int cxy = Integer.valueOf(counts[1].split("=")[1]);
                out.collect(totalFgBigrams, new LongWritable(cxy));
                out.collect(totalBgBigrams, new LongWritable(bxy));
                out.collect(uniqueFgBigrams, one);
                out.collect(uniqueBgBigrams, one);
            } else {
                int cx = Integer.valueOf(counts[1].split("=")[1]);
                out.collect(totalFgUnigrams, new LongWritable(cx));
                out.collect(uniqueFgUnigrams, one);
            }
        }
    }

    public static class CountSizeReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterator<LongWritable> values,
                           OutputCollector<Text, LongWritable> context,
                           Reporter reporter) throws IOException {
            long sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            context.collect(key, new LongWritable(sum));
        }
    }

}
