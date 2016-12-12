import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ViolationsMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private String year;
    private String month;
    private String precinct;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String[] parts = fileName.split("--");

        year = parts[0];
        month = parts[1];
        precinct = parts[2].replaceAll("\\D+", "");
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
        
        context.write(new Text("Precinct"), new Text(precinct));
        context.write(new Text("Month"), new Text(month));
        context.write(new Text("Year"), new Text(year));
        context.write(new Text("Violation"), new Text(record[index]));
        context.write(new Text("Amount"), new Text(record[index + 1]));
    }
}