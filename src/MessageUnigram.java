import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by vijay on 3/4/15.
 */
public class MessageUnigram {

    public static class MessageMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text,
                Text> out, Reporter reporter) throws IOException {
            // Maps bigram xy to x, bigram and y, bigram
            String[] tokens = value.toString().split("\t");
            String phrase = tokens[0];
            boolean isBigram = phrase.contains(" ");

            if (isBigram) {
                // Output x,bigram
                // Output y,bigram
                String[] unigrams = phrase.split(" ");
                out.collect(new Text(unigrams[0]), new Text(phrase));
                out.collect(new Text(unigrams[1]), new Text(phrase));
            } else {
                // Output: unigram   Bx=___,Cx=____  (identity function)
                out.collect(new Text(phrase), new Text(tokens[1]));
            }
        }
    }

    public static class MessageReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> out,
                           Reporter reporter) throws IOException {
            int bgCount = 0, fgCount = 0;
            Set<String> bigrams = new HashSet<String>();
            
            while (values.hasNext()) {
                String v = values.next().toString();
                // These are the unigram counts
                if (v.contains(",")) {
                    String[] tokens = v.split(",");
                    bgCount = Integer.valueOf(tokens[0].split("=")[1]);
                    fgCount = Integer.valueOf(tokens[1].split("=")[1]);
                } else {
                    bigrams.add(v);
                }
            }

            String keyString = key.toString();

            // Result: bigram -> bx=___,cx=____ or by=____,cy=____
            for (String bigram : bigrams) {
                String[] unigrams = bigram.split(" ");
                if (unigrams[0].equals(keyString)) {
                    out.collect(new Text(bigram), new Text(String.format("Bx=%d,Cx=%d", bgCount, fgCount)));
                }
                if (unigrams[1].equals(keyString)) {
                    out.collect(new Text(bigram), new Text(String.format("By=%d,Cy=%d", bgCount, fgCount)));
                }
            }

        }
    }

    public static class IdentityMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text,
                Text> out, Reporter reporter) throws IOException {
            // Maps bigram xy to x, bigram and y, bigram
            String[] tokens = value.toString().split("\t");
            String phrase = tokens[0];
            boolean isBigram = phrase.contains(" ");

            if (isBigram) {
                out.collect(new Text(tokens[0]), new Text(tokens[1]));
            }
        }
    }

    public static class ConcatenateReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> out,
                           Reporter reporter) throws IOException {
            String xCounts = "";
            String yCounts = "";
            String xyCounts = "";
            while (values.hasNext()) {
                String v = values.next().toString();
                if (v.startsWith("Bx=")) {
                    xCounts = v;
                } else if (v.startsWith("By=")) {
                    yCounts = v;
                } else if (v.startsWith("Bxy=")) {
                    xyCounts = v;
                }
            }
            
            out.collect(key, new Text(String.format("%s %s %s", xCounts, xyCounts, yCounts)));
        }
    }

}
