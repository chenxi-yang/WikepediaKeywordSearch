package cxyang;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFCalculation {

    public static class TFMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        //private LongArrayWritable result = new LongArrayWritable();
        //key: the position of this xml in the entire xml
        //value: all the xml of this key

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String xml = value.toString();

            String text = getTagContent(xml, "<text", "</text>");
            if (text.length() < 1){
                return;
            }

            String id = getTagContent(xml, "<id>", "</id>");
            if (id.length() < 1){
                return;
            }

            String title = getTagContent(xml, "<title>", "</title>");
            if (title.length() < 1){
                return;
            }

            char NomalizedText[] = text.toLowerCase().toCharArray();
            char NomalizedTitle[] = title.toLowerCase().toCharArray();

            StringNormalization(NomalizedText);
            StringNormalization(NomalizedTitle);

            IntWritable one = new IntWritable(1);

            Integer TitleWeight = 5;
            StringTokenizer itrTitle = new StringTokenizer(String.valueOf(NomalizedTitle));
            while(itrTitle.hasMoreTokens()){
                String word = itrTitle.nextToken();
                for(int i = 0; i < TitleWeight; i += 1){
                    String pairIdWord = id.concat(",").concat(word);
                    context.write(new Text(pairIdWord), one);
                }
            }

            //word in content
            StringTokenizer itrText = new StringTokenizer(String.valueOf(NomalizedText));
            while(itrText.hasMoreTokens()){
                String word = itrText.nextToken();
                String pairIdWord = id.concat(",").concat(word);
                context.write(new Text(pairIdWord), one);
            }
        }

        private String getTagContent(String txt, String tag1, String tag2){
            int l = txt.indexOf(tag1);
            int r;

            if(tag2.equals("</text>")){
                r = txt.lastIndexOf(tag2);
            }else{
                r = txt.indexOf(tag2);
            }
            if (r<=0) return "";

            while(txt.charAt(l) != '>'){
                l++;
            }

            return txt.substring(l+1, r);
        }

        private void StringNormalization(char[] text) {

            int len = text.length;

            for (int i = 0; i < len; i += 1){
                if(!((text[i] <= 'z' && text[i] >= 'a') || (text[i] <= 'Z' && text[i] >= 'A'))){
                    text[i] = ' ';
                }
            }
        }
    }

    public static class TFReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
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
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf, "FreqCal 别挂嘤嘤嘤");
        job.setJarByClass(TFCalculation.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(TFMap.class);
        job.setReducerClass(TFReduce.class);

        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
