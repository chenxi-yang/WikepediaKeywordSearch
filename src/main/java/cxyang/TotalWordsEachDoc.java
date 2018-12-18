package cxyang;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wikiclean.WikiClean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class TotalWordsEachDoc {
    public static class Map extends Mapper<Object, Text, LongWritable, Text> {
        //private LongArrayWritable result = new LongArrayWritable();
        //key: the position of this xml in the entire xml
        //value: all the xml of this key
        //
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            ArrayList<String> lineContent = new ArrayList<String>();

            while(itr.hasMoreTokens()){
                lineContent.add(itr.nextToken());
            }

            String pairIdWord = lineContent.get(0);
            String stringFreq = lineContent.get(1);

            String[] parts = pairIdWord.split(",");
            String stringId = parts[0];
            String word = parts[1];
            Long longId = Long.parseLong(stringId);

            LongWritable id = new LongWritable(longId);
            IntWritable freq = new IntWritable(1);
            String pairWordFreq = word.concat(",").concat(stringFreq);

            //(id, (word + freq))
            context.write(id, new Text(pairWordFreq));
        }
    }

    public static class Reduce extends Reducer<LongWritable, Text, Text, DoubleWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;

            ArrayList<Pair<String, Integer>> pairWordFreq = new ArrayList<Pair<String, Integer>>();

            for (Text val : values) {
                String stringPairWordFreq = val.toString();
                String[] parts = stringPairWordFreq.split(",");

                String word = parts[0];
                int freq = Integer.parseInt(parts[1]);
                pairWordFreq.add(new Pair<String, Integer>(word, freq));
                sum += freq;
            }
            result.set(sum);

            for(Pair<String, Integer> pair : pairWordFreq){
                String word = pair.getKey();
                Integer freq = pair.getValue();

                DoubleWritable tf = new DoubleWritable(freq * 1.000 / sum);
                String pairIdWord = key.toString().concat(",").concat(word);

                context.write(new Text(pairIdWord), tf);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TF Calculation");
//        job.getConfiguration().setStrings("mapreduce.reduce.shuffle.memory.limit.percent", "0.15");

        job.setJarByClass(TotalWordsEachDoc.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
