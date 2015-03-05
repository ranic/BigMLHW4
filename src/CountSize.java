import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by vijay on 3/3/15.
 */
public class CountSize {

    public static class CountSizeMapper extends
            Mapper<LongWritable, Text, Text, LongWritable> {

        Text totalFgBigrams = new Text("totalFgBigrams");
        Text totalBgBigrams = new Text("totalBgBigrams");
        Text totalFgUnigrams = new Text("totalFgUnigrams");
        Text uniqueFgBigrams = new Text("uniqueFgBigrams");
        Text uniqueBgBigrams = new Text("uniqueBgBigrams");
        Text uniqueFgUnigrams = new Text("uniqueFgUnigrams");
        LongWritable one = new LongWritable(1);

        @Override
        public void map(LongWritable longWritable, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String phrase = tokens[0];
            String[] counts = tokens[1].split(",");
            boolean isBigram = phrase.contains(" ");

            if (isBigram) {
                int bxy = Integer.valueOf(counts[0].split("=")[1]);
                int cxy = Integer.valueOf(counts[1].split("=")[1]);
                context.write(totalFgBigrams, new LongWritable(cxy));
                context.write(totalBgBigrams, new LongWritable(bxy));
                context.write(uniqueFgBigrams, one);
                context.write(uniqueBgBigrams, one);
            } else {
                int cx = Integer.valueOf(counts[1].split("=")[1]);
                context.write(totalFgUnigrams, new LongWritable(cx));
                context.write(uniqueFgUnigrams, one);
            }
        }
    }

    public static class CountSizeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

}
