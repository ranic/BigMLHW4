import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by vijay on 3/3/15.
 */
public class Aggregate {
    private static final String FOREGROUND_DECADE = "1960";
    private static String[] STOP_WORD_ARRAY = {"i", "the", "to", "and", "a", "an", "of", "it", "you", "that", "in", "my", "is", "was", "for"};
    private static final Set<String> STOP_WORDS = new HashSet<String>(Arrays.asList(STOP_WORD_ARRAY));

    private static boolean containsStopWords(String phrase) {
        String[] tokens = phrase.split(" ");
        for (String token : tokens) {
            if (STOP_WORDS.contains(token))
                return true;
        }
        return false;
    }

    public static class AggregateMapper extends Mapper<LongWritable, Text, Text, Text> {


        private Text formatCount(boolean isForeground, boolean isBigram, int count) {
            if (isBigram) {
                return new Text(String.format(isForeground ? "Cxy=%d" : "Bxy=%d", count));
            } else {
                return new Text(String.format(isForeground ? "Cx=%d" : "Bx=%d", count));
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Parse the line
            String[] tokens = value.toString().split("\t");
            String phrase = tokens[0];
            String decade = tokens[1];
            int count = Integer.valueOf(tokens[2]);

            // Output the appropriate foreground or background count string
            if (!containsStopWords(phrase)) {
                boolean isBigram = phrase.contains(" ");
                boolean isForeground = decade.equals(FOREGROUND_DECADE);

                context.write(new Text(phrase), formatCount(isForeground, isBigram, count));
            }
        }
    }

    public static class AggregateReducer extends
            Reducer<Text, Text, Text, Text> {

        private Text formatOutput(boolean isBigram, int bgCount, int fgCount) {
            if (isBigram) {
                return new Text(String.format("Bxy=%d,Cxy=%d", bgCount, fgCount));
            } else {
                return new Text(String.format("Bx=%d,Cx=%d", bgCount, fgCount));
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
             /* Values are counts of form: Bxy=%d, Cxy=%d. Need to collect and reduce*/
            int bgCount = 0;
            int fgCount = 0;
            boolean isBigram = key.toString().contains(" ");
            String countType;
            int count;
            for (Text value : values) {
                String[] tokens = value.toString().split("=");
                countType = tokens[0];
                count = Integer.valueOf(tokens[1]);

                if (isBigram) {
                    if (countType.equals("Bxy"))
                        bgCount += count;
                    else if (countType.equals("Cxy"))
                        fgCount += count;
                } else {
                    if (countType.equals("Bx"))
                        bgCount += count;
                    else if (countType.equals("Cx"))
                        fgCount += count;
                }
            }
            context.write(key, formatOutput(isBigram, bgCount, fgCount));

        }
    }

}
