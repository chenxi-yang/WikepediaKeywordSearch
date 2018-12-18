// ref: https://github.com/lintool/wikiclean
package cxyang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wikiclean.WikiClean;

import java.io.IOException;

class LongArrayWritable extends ArrayWritable {
    public LongArrayWritable() {
        super(LongWritable.class);
    }

    public LongArrayWritable(LongWritable[] arrays) {
        super(LongWritable.class);
        set(arrays);
    }

    @Override
    public String toString() {
        String[] strs = toStrings();
        String res = "";
        for (int i = 0; i < strs.length; i++) {
            res += strs[i];
            if (i + 1 < strs.length) res += "\t";
        }
        return res;
    }
}

public class Position {
    //public static final String INPUT_PATH = "/Users/cxyang/Documents/edu/fudan/this\\ semester/distributedSystem/PJ/HelloHadoop/sample_input.xml";
    //public static final String OUTPUT_PATH = "/Users/cxyang/Documents/edu/fudan/this\\ semester/distributedSystem/PJ/HelloHadoop/results_position";
    public static class Map extends Mapper<LongWritable, Text, LongWritable, LongArrayWritable> {
        private LongArrayWritable result = new LongArrayWritable();
        //key: the position of this xml in the entire xml
        //value: all the xml of this key

        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String text = value.toString();
            WikiClean cleaner = new WikiClean.Builder().withTitle(true).build();
            LongWritable id = new LongWritable(Long.parseLong(cleaner.getId(text)));
            LongWritable len = new LongWritable(value.getLength());
            //System.out.println("here");
            if (id.get() == -1) return;
            //(id, offset, length)
            context.write(id, new LongArrayWritable(new LongWritable[]{key, len}));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf, "position");
        job.setJarByClass(Position.class);
        job.setMapperClass(Position.Map.class);

        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongArrayWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
