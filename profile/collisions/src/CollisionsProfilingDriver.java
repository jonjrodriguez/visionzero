import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CollisionsProfilingDriver
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 2) {
            System.err.println("Usage: CollisionsProfilingDriver <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(CollisionsProfilingDriver.class);
        job.setJobName("Collision Profiling");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CollisionsMapper.class);
        job.setReducerClass(CollisionsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(ExcelInputFormat.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}