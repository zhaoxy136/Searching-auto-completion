import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.*;

/**
 * Created by barryzhao on 10/12/16.
 */
public class LanguageModel {
    public static class ModelMapper extends Mapper<LongWritable, Text, Text, Text> {

        int threshold;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int threshold = conf.getInt("threshold", 20);
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws
                IOException, InterruptedException {
            if (values == null || values.toString().trim().length() == 0) {
                return;
            }

            String line = values.toString().trim();
            String[] wordsPlusCount = line.split("\t");
            int count = Integer.valueOf(wordsPlusCount[1]);
            //int count = Integer.parseInt(wordsPlusCount[1]);
            if (count < threshold) {
                return;
            }

            String[] words = wordsPlusCount[0].split("\\s+");
            StringBuilder outputKey = new StringBuilder();
            for (int i = 0 ; i < words.length-1; i++) {
                outputKey.append(words[i]).append(" ");
            }
            StringBuilder outputVal = new StringBuilder();
            outputVal.append(words[words.length-1]).append("=").append(count);
            context.write(new Text(outputKey.toString().trim()), new Text(outputVal.toString()));
        }

    }

    public static class ModelReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        int n;
        //select top K for each key
        //get top K
        //select top K from value list
        //write to database
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("n", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws
                IOException, InterruptedException {
            TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
            for (Text val : values) {
                String[] curValue = val.toString().split("=");
                String word = curValue[0];
                int count = Integer.parseInt(curValue[1]);
                //tm.putIfAbsent(count, new ArrayList<String>());
                if (!tm.containsKey(count)) tm.put(count, new ArrayList<String>());
                tm.get(count).add(word);
            }
            Iterator<Integer> iter = tm.keySet().iterator();
            for (int j = 0; iter.hasNext() && j < n; j++) {
                int count = iter.next();
                List<String> words = tm.get(count);
                for (String curWord : words) {
                    //database starting_phrase following_word count
                    context.write(new DBOutputWritable(key.toString(), curWord, count), NullWritable.get());
                }
            }

        }

    }
}
