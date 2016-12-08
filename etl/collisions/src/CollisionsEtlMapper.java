import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;

public class CollisionsEtlMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
    private String borough;
    private String year;
    private String month;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String[] parts = fileName.split("--");

        if ((fileName.contains("bk")) || (fileName.contains("bn"))) {
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
        fileDate.set(Integer.parseInt(year), Integer.parseInt(month) - 1, 1);

        String delim = ",";
        // Borough, Month and Year
  	  	sb.append(borough).append(delim).append(month).append(delim).append(year);
  	  	
        // If the file's date is before March 2014, data is using the old format
        // Else the data is using the new format
        if (fileDate.before(newDataFormatDate)) {
        	if (!line.contains("Address") && ((columns.length == 10) || (columns.length == 9))) {
            	String[] injuredColumns = columns[6].split(";");
            	String[] killedColumns = columns[7].split(";");
        		
            	// Precinct
            	if (columns[0].matches(".*[0-9].*")) {
            		columns[0] = columns[0].replaceAll("[^0-9?!\\.]","");
                	columns[0] = String.format("%03d", Integer.parseInt(columns[0]));
            	}
            	if (columns[0].toLowerCase().contains("south")) {
            		sb.append(delim).append("014");
            	}
            	else if (columns[0].toLowerCase().contains("north")) {
            		sb.append(delim).append("018");
            	}
            	else {
            		sb.append(delim).append(columns[0]);
            	}
            	
            	// CollisionCount
            	sb.append(delim).append(columns[2]);
            	
            	// CollisionInjuredCount
            	if (injuredColumns.length == 5) {
            		sb.append(delim).append(injuredColumns[4]);
            	}
            	else {
            		sb.append(delim).append("0");
            	}
            	
            	// CollisionKilledCount
            	if (killedColumns.length == 5) {
            		sb.append(delim).append(killedColumns[4]);
            	}
            	else {
            		sb.append(delim).append("0");
            	}
            	
            	// Motorists, Passengers, Cyclists, Pedestrians injured and killed
            	for (int i = 0; i < 4; i++) {
            		if ((injuredColumns.length == 5) && (killedColumns.length == 5)) {
            			sb.append(delim).append(injuredColumns[i]);
            		}
            		else {
            			sb.append(delim).append("0");
            		}
            	}
            	
        	}
        	else {
        		return;
        	}
        }
        else {
          if (!line.contains("Collision") && (columns.length == 23))  {
        	  Integer[] ignoredColumns = {1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 21, 22};

        	  int i = 0;
        	  for (String col : columns) {
        		  if (!Arrays.asList(ignoredColumns).contains(i)) {
        			  if (i == 8) {
        				  sb.append(delim).append("1"); 
        			  }
        			  sb.append(delim).append(col);
        		  }
        		  i++;
        	  }
          }
          else {
            return;
          }
        }

    	  context.write(NullWritable.get(), new Text(sb.toString()));
    }
}
