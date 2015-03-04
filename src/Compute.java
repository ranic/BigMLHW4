import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by vijay on 3/4/15.
 */
public class Compute {
    public static class ComputeMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        private static final String SIZE_COUNT_PARAM = "sizeCountFile";
        private static final Pattern ATTR_PATTERN = Pattern.compile("Bx=(\\d+),Cx=(\\d+) Bxy=(\\d+),Cxy=(\\d+) By=(\\d+),Cy=(\\d+)");
        private static final String OUTPUT_FORMAT = "%s\t%s\t%s";

        long totalBgBigrams = 0;
        long totalFgBigrams = 0;
        long totalFgUnigrams = 0;
        long uniqueBgBigrams = 0;
        long uniqueFgBigrams = 0;
        long uniqueFgUnigrams = 0;

        private long getSizeCount(String line) {
            return Long.valueOf(line.split("\t")[1]);
        }

        @Override
        public void configure(JobConf job) {
            String sizeCountFilename = job.get(SIZE_COUNT_PARAM);
            Path path = new Path(sizeCountFilename);
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                totalBgBigrams = getSizeCount(br.readLine());
                totalFgBigrams = getSizeCount(br.readLine());
                totalFgUnigrams = getSizeCount(br.readLine());
                uniqueBgBigrams = getSizeCount(br.readLine());
                uniqueFgBigrams = getSizeCount(br.readLine());
                uniqueFgUnigrams = getSizeCount(br.readLine());
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text,
                Text> out, Reporter reporter) throws IOException {
            // Parse out phrases -> attributes pair
            String[] tokens = value.toString().split("\t");
            String curPhrase = tokens[0];
            String attributes = tokens[1];
            // Output phrase -> score
            out.collect(new Text(curPhrase), computeScore(curPhrase, attributes));
        }

        private Text computeScore(String curPhrase, String attributes) {
            Matcher m = ATTR_PATTERN.matcher(attributes);
            if (!m.matches()) {
                System.out.println("Sac: " + curPhrase + " " + attributes);
                return new Text();
            }
            double bx,cx,bxy,cxy,by,cy;

            bx = Double.valueOf(m.group(1));
            cx = Double.valueOf(m.group(2));
            bxy = Double.valueOf(m.group(3));
            cxy = Double.valueOf(m.group(4));
            by = Double.valueOf(m.group(5));
            cy = Double.valueOf(m.group(6));

            double pLog = Math.log(cxy + 1) - Math.log(totalFgBigrams + uniqueFgBigrams);
            double qPhrasenessLog = Math.log(cx + 1)-Math.log(totalFgUnigrams + uniqueFgUnigrams) + Math.log(cy + 1) - Math.log(totalFgUnigrams + uniqueFgUnigrams);
            double qInformativenessLog = Math.log(bxy+1) - Math.log(totalBgBigrams + uniqueBgBigrams);

            double phraseness = Math.exp(pLog) * (pLog - qPhrasenessLog);
            double informativeness = Math.exp(pLog) * (pLog - qInformativenessLog);

            double score = phraseness + informativeness;
            return new Text(String.format(OUTPUT_FORMAT, score, phraseness, informativeness));
        }
    }

    public static class ComputeReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> out,
                           Reporter reporter) throws IOException {
            // Identity Reducer: Just outputs the first value from values
            if (values.hasNext())
                out.collect(key, values.next());
        }
    }

}
