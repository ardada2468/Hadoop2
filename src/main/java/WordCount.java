import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import jdk.nashorn.internal.parser.Token;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * TO RUN
 *  swich user to hdoop
 *  Remove previos output data or change path
 *  hadoop fs -rm /WordCountTutorial/Output/_SUCCESS
 *  hadoop fs -rm /WordCountTutorial/Output/part-r-00000
 *   hadoop fs -rm -r /WordCountTutorial/Output/
 *  hadoop jar '/home/arnavdadarya/IdeaProjects/Hadoop2/out/artifacts/Hadoop2_jar/Hadoop2.jar' /WordCountTutorial/Input /WordCountTutorial/Output
 */

public class WordCount {
    static ArrayList<Character> punctuation = new ArrayList<>();
    static {
        punctuation.add('\'');
        punctuation.add('{');
        punctuation.add('}');
        punctuation.add('(');
        punctuation.add(')');
        punctuation.add('<');
        punctuation.add('>');
        punctuation.add(':');
        punctuation.add(',');
        punctuation.add('-');
        punctuation.add('\"');
        punctuation.add('?');
        punctuation.add('.');
        punctuation.add('!');
        punctuation.add('_');
        punctuation.add(']');
        punctuation.add('[');

    }


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(removePunctuation(itr.nextToken()));
                context.write(word, one);
            }
        }
    }

    public static String removePunctuation(String x){
        char[] chars = x.toCharArray();
        StringBuilder Returnable = new StringBuilder();
        for (int i = 0; i < chars.length; i++) {

            if(!punctuation.contains((chars[i]))){
                Returnable.append(String.valueOf(chars[i]));
            }else {
                Returnable.append("");
            }
        }
        return Returnable.toString();
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}