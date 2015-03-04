import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

/**
 * Created by vijay on 3/3/15.
 */
public class Aggregate {
    private static final String bigramFormat = "%s\tBxy=%d,Cxy=%d\n";
    private static final String unigramFormat = "%s\tBx=%d,Cx=%d\n";
    private static final String FOREGROUND_DECADE = "1960";
    private static String[] STOP_WORD_ARRAY = {"i", "the", "to", "and", "a", "an", "of", "it", "you", "that", "in", "my", "is", "was", "for"};
    private static final Set<String> STOP_WORDS = new HashSet<String>(Arrays.asList(STOP_WORD_ARRAY));

    static boolean containsStopWords(String phrase) {
        String[] tokens = phrase.split(" ");
        for (String token : tokens) {
            if (STOP_WORDS.contains(token))
                return true;
        }
        return false;
    }

    public static class BigramMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {
        private static final String FOREGROUND_DECADE = "1960";

        int foreCount = 0;
        int backCount = 0;
        long uniqueFgBigrams = 0;
        long totalFgBigrams = 0;
        long uniqueBgBigrams = 0;
        long totalBgBigrams = 0;

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text,
                Text> outputCollector, Reporter reporter) throws IOException {

            // Parse the line
            String[] tokens = value.toString().split("\t");
            String phrase = tokens[0];
            String decade = tokens[1];
            int count = Integer.valueOf(tokens[2]);

            // Output the appropriate foreground or background count string
            if (!containsStopWords(phrase)) {
                boolean isForeground = decade.equals(FOREGROUND_DECADE);
                Text output = new Text(String.format(isForeground ? "Cxy=%d" : "Bxy=%d", count));

                outputCollector.collect(new Text(phrase), output);
            }
        }
/*
            // New phrase; flush the counts of the previous phrase
            if (!curPhrase.equals(prevPhrase)) {
                if (prevPhrase == null) {
                    prevPhrase = curPhrase;
                } else {
                    counts = new Text(createCountString(true, prevPhrase, backCount, foreCount));
                    outputCollector.collect(new Text(prevPhrase), counts);
                    // Update total counts for probability computation and smoothing
                    if (foreCount > 0) {
                        uniqueFgBigrams++;
                        totalFgBigrams += foreCount;
                    }
                    if (backCount > 0) {
                        uniqueBgBigrams++;
                        totalBgBigrams += foreCount;
                    }
                     if (foreCount > 0) {
                            uniqueFgUnigrams++;
                            totalFgUnigrams += foreCount;
                        }
                    }

                    // Reset local variables
                    prevPhrase = curPhrase;
                    foreCount = 0;
                    backCount = 0;
                }
            }
*/
    }


    public static class UnigramMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        String prevPhrase = null, curPhrase = null;
        int foreCount = 0;
        int backCount = 0;
        long uniqueFgUnigrams = 0;
        long totalFgUnigrams = 0;

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text,
                Text> outputCollector, Reporter reporter) throws IOException {

            // Parse the line
            String[] tokens = value.toString().split("\t");
            String phrase = tokens[0];
            String decade = tokens[1];
            int count = Integer.valueOf(tokens[2]);

            // Output the appropriate foreground or background count string
            if (!containsStopWords(phrase)) {
                boolean isForeground = decade.equals(FOREGROUND_DECADE);
                Text output = new Text(String.format(isForeground ? "Cx=%d" : "Bx=%d", count));

                outputCollector.collect(new Text(phrase), output);
            }
        }
    }


    public static class BigramReducer extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> context,
                           Reporter reporter) throws IOException {
             /* Values are counts of form: Bxy=%d, Cxy=%d. Need to collect and reduce*/
            int bgCount = 0;
            int fgCount = 0;
            String countType;
            int count;
            while (values.hasNext()) {
                String[] tokens = values.next().toString().split("=");
                countType = tokens[0];
                count = Integer.valueOf(tokens[1]);
                if (countType.equals("Bxy"))
                    bgCount += count;
                else if (countType.equals("Cxy"))
                    fgCount += count;
            }

            context.collect(key, new Text(String.format("Bxy=%d,Cxy=%d", bgCount, fgCount)));
        }
    }

    public static class UnigramReducer extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> context,
                           Reporter reporter) throws IOException {
             /* Values are counts of form: Bxy=%d, Cxy=%d. Need to collect and reduce*/
            int bgCount = 0;
            int fgCount = 0;
            String countType;
            int count;
            while (values.hasNext()) {
                String s = values.next().toString();
                System.out.println("sac: " + key.toString() + " " + s);
                String[] tokens = s.split("=");
                countType = tokens[0];
                count = Integer.valueOf(tokens[1]);
                if (countType.equals("Bx"))
                    bgCount += count;
                else if (countType.equals("Cx"))
                    fgCount += count;
            }

            context.collect(key, new Text(String.format("Bx=%d,Cx=%d", bgCount, fgCount)));
        }
    }

}
