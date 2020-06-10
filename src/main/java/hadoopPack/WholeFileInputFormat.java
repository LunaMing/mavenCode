package hadoopPack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public SingleFileNameReader createRecordReader(
            InputSplit split, TaskAttemptContext context)throws IOException,InterruptedException{
        return new SingleFileNameReader((FileSplit)split, context.getConfiguration());
    }

    @Override
    public RecordReader<Text, BytesWritable> getRecordReader(InputSplit arg0, JobConf arg1, Reporter arg2)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
}
