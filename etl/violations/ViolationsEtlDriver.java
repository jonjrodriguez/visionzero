import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ViolationsEtlDriver extends Configured implements Tool
{
    public static void main(String[] args) throws Exception
    {
        if (args.length < 2) {
            System.err.println("Usage: ViolationsEtlDriver <input path> <output path>");
            System.exit(-1);
        }

        int res = ToolRunner.run(new Configuration(), new ViolationsEtlDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = new Job(conf);
        job.setJarByClass(ViolationsEtlDriver.class);
        job.setJobName("ViolationsEtlDriver");

        job.setMapperClass(ViolationsEtlMapper.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(ExcelInputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
