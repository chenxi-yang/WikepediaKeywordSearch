package cxyang;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

class ValueComparator implements Comparator<String> {
    private Map<String, Double> base;

    public ValueComparator(Map<String, Double> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with
    // equals.
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return 1;
        } else {
            return -1;
        } // returning 0 would merge keys
    }
}

public class PreRank {
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

            String pairWordId = lineContent.get(0);
            String stringTFIDF = lineContent.get(1);

            String[] parts = pairWordId.split(",");
            String word = parts[0];
            String stringId = parts[1];

            Double doubleTfidf = Double.parseDouble(stringTFIDF);

            String pairIdTfidf = stringId.concat(",").concat(doubleTfidf.toString());

            context.write(new Text(word), new Text(pairIdTfidf));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        final static double topNum = 20;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {

            HashMap<String, Double> mapIdTfidf = new HashMap<String, Double>();
            ValueComparator bvc = new ValueComparator(mapIdTfidf);
            TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);
            String result = "";

            int sum = 0;

            //word, (id, tfidf)
            for (Text val : values) {
                String stringPairIdFreq = val.toString();
                String[] parts = stringPairIdFreq.split(",");

                String Id = parts[0];
                Double Tfidf = Double.parseDouble(parts[1]);

                mapIdTfidf.put(Id, Tfidf);
                sum += 1;
            }

            //for each word, sorted(id, tfidf)
            sorted_map.putAll(mapIdTfidf);

            ArrayList<String> listKey = new ArrayList<String>(sorted_map.keySet());

            int len = listKey.size();

            for(int i = 0; i < topNum; i++){
                if(i >= len) break;
                if(i == 0){
                    result = result.concat(listKey.get(i));
                }else{
                    result = result.concat(",").concat(listKey.get(i));
                }
            }
            context.write(key, new Text(result));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Pre Rank Ying Ying Ying");
        job.setJarByClass(TFIDFCalculation.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(PreRank.Map.class);
        job.setReducerClass(PreRank.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
