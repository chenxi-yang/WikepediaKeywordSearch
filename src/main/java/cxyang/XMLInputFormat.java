// source: https://www.iteblog.com/archives/1596.html
package cxyang;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class XMLInputFormat extends TextInputFormat {
    // private static final Logger log = LoggerFactory.getLogger(XMLInputFormat.class);
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext context) {
        try {
            return new XMLRecordReader(inputSplit, context.getConfiguration());
        } catch (IOException e) {
            // log.warn("Error while creating XmlRecordReader", e);
            return null;
        }
    }
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        // TODO Auto-generated method stub
        return super.isSplitable(context, file);
    }
}
