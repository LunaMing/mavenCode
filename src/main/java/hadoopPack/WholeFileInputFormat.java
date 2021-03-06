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


/**
 *  重写FileInputFormat，将文件不分割，读入到一个map 
 *  
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        // TODO Auto-generated method stub
        return new SingleFileNameReader((FileSplit) split);

    }
}
