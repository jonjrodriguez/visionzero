import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ViolationsEtlMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
    private String year;
    private String month;
    private String precinct;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path filePath = ((FileSplit) context.getInputSplit()).getPath();

        int depth = filePath.depth();
        String path = filePath.toString();
        String[] parts = path.split(Path.SEPARATOR);

        year = parts[depth - 2];
        month = parts[depth - 1];
        precinct = filePath.getName().replaceAll("\\D+", "");
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] record = line.split(",");

        int index = -1;
        for (int i = 0; i < record.length; i++) {
            if (record[i].equalsIgnoreCase("Speeding")) {
                index = i;
                break;
            }
        }

        if (index == -1) {
            return;
        }

        // precinct, month, year, violation, amount
        String results = precinct + "," + month + "," + year + "," + record[index] + "," + record[index + 1];

        context.write(NullWritable.get(), new Text(results));
    }
}
