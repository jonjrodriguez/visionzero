import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Calendar;

public class EtlMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
    private String borough;
    private String year;
    private String month;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String[] parts = fileName.split("--");

        if (fileName.contains("bk")) {
            borough = "Brooklyn";
        }
        else if (fileName.contains("bx")) {
        	borough = "Bronx";
        }
        else if (fileName.contains("mn")) {
        	borough = "Manhattan";
        }
        else if (fileName.contains("qn")) {
        	borough = "Queens";
        }
        else if (fileName.contains("si")) {
        	borough = "Staten Island";
        }

        year = parts[0];
        month = parts[1];
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",");
        StringBuilder sb = new StringBuilder();

        // The data is of a different format before March 2014
        Calendar newDataFormatDate = Calendar.getInstance();
        newDataFormatDate.set(2014, 02, 1);
        Calendar fileDate = Calendar.getInstance();
        fileDate.set(Integer.parseInt(year), Integer.parseInt(month), 1);

        // If the file's date is before March 2014, data is using the old format
        // Else the data is using the new format
        if (fileDate.before(newDataFormatDate)) {
        	// TODO: Figure out the best way to parse and clean the old formatted data
        	/*
        	if (columns.length == 8) {
        		String delim = ",";
        	}
        	else {
        		return;
        	}

        	context.write(NullWritable.get(), new Text(sb.toString()));
        	*/
        	return;
        }
        else {
          if (!line.contains("Collision") && (columns.length == 23))  {
            String delim = ",";
            sb.append(borough).append(delim).append(month).append(delim).append(year);
            int[] ignoredColumns = {1, 2, 3, 7, 10};

            int i = 0;
	          for (String col : columns) {
              if (ArrayUtils.contains(ignoredColumns, i)) {
                sb.append(delim).append(col);
             		i++;
            	}
            }
          }
          else {
            return;
          }

    	    context.write(NullWritable.get(), new Text(sb.toString()));
        	 if (!line.contains("Collision") && (columns.length == 23))  {
		     String delim = ",";
	             sb.append(borough).append(delim).append(month).append(delim).append(year);
	             int[] ignoredColumns = {1, 2, 3, 7, 10};
	             
	             int i = 0;
	             for (String col : columns) {
	            	 if (ArrayUtils.contains(ignoredColumns, i)) {
	            		 sb.append(delim).append(col);
	             		 i++;
	            	 }
	             }
             }
             else {
             	return;
             }
        	 
        	 context.write(NullWritable.get(), new Text(sb.toString()));
        }
    }
}
