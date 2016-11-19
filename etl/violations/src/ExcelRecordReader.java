import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ExcelRecordReader extends RecordReader<LongWritable, Text>
{
    private FSDataInputStream fileIn;
    private String sheet;
    private LongWritable key;
    private Text value;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException
    {
        FileSplit split = (FileSplit) genericSplit;
        Configuration conf = context.getConfiguration();
        final Path file = split.getPath();

        FileSystem fs = file.getFileSystem(conf);
        fileIn = fs.open(split.getPath());

        sheet = new ExcelParser().parseExcelData(fileIn);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        if (key == null) {
            key = new LongWritable(0);
            value = new Text(sheet);
        } else {
            return false;
        }

        if (key == null || value == null) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException
    {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {
        if (fileIn != null) {
            fileIn.close();
        }
    }
}