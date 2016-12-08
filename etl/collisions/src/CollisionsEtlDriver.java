import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CollisionsEtlDriver
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 2) {
            System.err.println("Usage: CollisionsEtlDriver <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(CollisionsEtlDriver.class);
        job.setJobName("ETL Collision Data");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CollisionsEtlMapper.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(ExcelInputFormat.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
