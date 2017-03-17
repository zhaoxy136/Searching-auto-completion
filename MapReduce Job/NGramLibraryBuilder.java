import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.InterruptedIOException;
import java.io.IOException;


/**
 * Created by barryzhao on 10/10/16.
 */
public class NGramLibraryBuilder {
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        //map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException {
            //ngram
            Configuration conf = context.getConfiguration();
            int nGram = conf.getInt("nGram", 5);
            String sentence = value.toString().toLowerCase().trim();
            //remove non-alphabetical symbols
            sentence = sentence.replaceAll("[^a-z]", " ");
            String[] words = sentence.split("\\s+");
            for (int i = 0; i < words.length; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append(words[i]);
                for (int j = 1; i + j < words.length && j < nGram; j++) {
                    sb.append(" ");
                    sb.append(words[i+j]);
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }

        }

    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : values) {
                sum += count.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}
