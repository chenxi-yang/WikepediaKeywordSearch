package cxyang;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class TFIDFCalculation {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        //private LongArrayWritable result = new LongArrayWritable();
        //key: the position of this xml in the entire xml
        //value: all the xml of this key

        @Override
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            ArrayList<String> lineContent = new ArrayList<String>();

            while(itr.hasMoreTokens()){
                lineContent.add(itr.nextToken());
            }

            String pairIdWord = lineContent.get(0);
            String stringTF = lineContent.get(1);

            String[] parts = pairIdWord.split(",");
            String stringId = parts[0];
            String word = parts[1];

            Double doubleTF = Double.parseDouble(stringTF);

            String pairIdFreq = stringId.concat(",").concat(doubleTF.toString());

            context.write(new Text(word), new Text(pairIdFreq));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
        private IntWritable result = new IntWritable();
        private Integer N = 2115037;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            ArrayList<Pair<String, Double>> pairIdFreq = new ArrayList<Pair<String, Double>>();

            int sum = 0;
            Double tfAverage = 0.0;
            for (Text val : values) {
                String stringPairIdFreq = val.toString();
                String[] parts = stringPairIdFreq.split(",");

                String Id = parts[0];
                Double TF = Double.parseDouble(parts[1]);

                pairIdFreq.add(new Pair<String, Double>(Id, TF));
                tfAverage += TF;
                System.out.println(TF);

                sum += 1;
            }

            tfAverage /= sum;

            for(Pair<String, Double> pair : pairIdFreq){
                String Id = pair.getKey();
                Double tf = pair.getValue();

                if(tf >= tfAverage){
                    DoubleWritable TFIDF = new DoubleWritable(tf * Math.log( N / sum));
                    String pairWordId = key.toString().concat(",").concat(Id);

                    context.write(new Text(pairWordId), TFIDF);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TF-IDF Calculation");
        job.setJarByClass(TFIDFCalculation.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(TFIDFCalculation.Map.class);
        job.setReducerClass(TFIDFCalculation.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
