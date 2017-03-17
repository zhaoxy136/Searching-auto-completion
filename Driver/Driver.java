import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by barryzhao on 10/13/16.
 */
public class Driver {
    //implements two drivers
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        //job1
        Configuration conf1 = new Configuration();
        conf1.set("textinputformat.record.delimiter", ".");
        conf1.set("nGram", args[2]);

        Job job1 = Job.getInstance(conf1);
        job1.setJobName("NGram");
        job1.setJarByClass(Driver.class);

        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(args[0]));
        TextOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("threshold", args[3]);
        conf2.set("n", args[4]);

        DBConfiguration.configureDB(conf2,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/nGramModel",
                "root",
                "123cc123");

        Job job2 = Job.getInstance(conf2);
        job2.setJobName("Model");
        job2.setJarByClass(Driver.class);

        job2.addArchiveToClassPath(new Path("path_to_ur_connector"));
        job2.setMapperClass(LanguageModel.ModelMapper.class);
        job2.setReducerClass(LanguageModel.ModelReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        DBOutputFormat.setOutput(job2,
                "output", new String[] {"startingPhrase", "followingWord", "count"});
        TextInputFormat.setInputPaths(job2, args[1]);
        job2.waitForCompletion(true);
    }

}
