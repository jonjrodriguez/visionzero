import java.io.IOException;
import java.util.Calendar;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CollisionsMapper
  extends Mapper<LongWritable, Text, Text, Text> {
	
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

      // The data is of a different format before March 2014
      Calendar newDataFormatDate = Calendar.getInstance();
      newDataFormatDate.set(2014, 02, 1);
      Calendar fileDate = Calendar.getInstance();
      fileDate.set(Integer.parseInt(year), Integer.parseInt(month) - 1, 1);

      // Borough, Month and Year
      context.write(new Text("Borough"), new Text(borough));
      context.write(new Text("Month"), new Text(month));
      context.write(new Text("Year"), new Text(year));
	  	
      // If the file's date is before March 2014, data is using the old format
      // Else the data is using the new format
      if (fileDate.before(newDataFormatDate)) {
      	if (!line.contains("Address") && ((columns.length == 10) || (columns.length == 9) || (columns.length == 8))) {
          	String[] injuredColumns = columns[6].split(";");
          	String[] killedColumns = columns[7].split(";");
      		
          	// Precinct
          	if (columns[0].matches(".*[0-9].*")) {
          		columns[0] = columns[0].replaceAll("[^0-9?!\\.]","");
              	columns[0] = String.format("%03d", Integer.parseInt(columns[0]));
          	}
          	if (columns[0].toLowerCase().contains("south")) {
          		context.write(new Text("Precinct"), new Text("014"));
          	}
          	else if (columns[0].toLowerCase().contains("north")) {
          		context.write(new Text("Precinct"), new Text("018"));
          	}
          	else {
          		context.write(new Text("Precinct"), new Text(columns[0]));
          	}
          	
          	// CollisionCount
          	context.write(new Text("CollisionCount"), new Text(columns[2]));
          	
          	// CollisionInjuredCount
          	if (injuredColumns.length == 5) {
          		context.write(new Text("CollisionInjuredCount"), new Text(injuredColumns[4]));
          	}
          	else {
          		context.write(new Text("CollisionInjuredCount"), new Text("0"));
          	}
          	
          	// CollisionKilledCount
          	if (killedColumns.length == 5) {
          		context.write(new Text("CollisionKilledCount"), new Text(killedColumns[4]));
          	}
          	else {
          		context.write(new Text("CollisionKilledCount"), new Text("0"));
          	}
          	
          	// Motorists, Passengers, Cyclists, Pedestrians injured and killed
          	if ((injuredColumns.length == 5) && (killedColumns.length == 5)) {
          		context.write(new Text("MotoristsInjured"), new Text(injuredColumns[0]));
          		context.write(new Text("MotoristsKilled"), new Text(killedColumns[0]));
          		context.write(new Text("PassengersInjured"), new Text(injuredColumns[1]));
          		context.write(new Text("PassengersKilled"), new Text(killedColumns[1]));
          		context.write(new Text("CyclistsInjured"), new Text(injuredColumns[2]));
          		context.write(new Text("CyclistsKilled"), new Text(killedColumns[2]));
          		context.write(new Text("PedestriansInjured"), new Text(injuredColumns[3]));
          		context.write(new Text("PedestriansKilled"), new Text(killedColumns[3]));
      		}
      		else {
      			context.write(new Text("MotoristsInjured"), new Text("0"));
          		context.write(new Text("MotoristsKilled"), new Text("0"));
          		context.write(new Text("PassengersInjured"), new Text("0"));
          		context.write(new Text("PassengersKilled"), new Text("0"));
          		context.write(new Text("CyclistsInjured"), new Text("0"));
          		context.write(new Text("CyclistsKilled"), new Text("0"));
          		context.write(new Text("PedestriansInjured"), new Text("0"));
          		context.write(new Text("PedestriansKilled"), new Text("0"));
      		}
      	}
      	else {
      		return;
      	}
      }
      else {
        if (!line.contains("Collision") && (columns.length == 23))  {
      	  	context.write(new Text("Precinct"), new Text(columns[0]));
      	  	context.write(new Text("CollisionInjuredCount"), new Text(columns[8]));
      	  	context.write(new Text("CollisionKilledCount"), new Text(columns[9]));
      	  	context.write(new Text("MotoristsInjured"), new Text(columns[13]));
    		context.write(new Text("MotoristsKilled"), new Text(columns[14]));
    		context.write(new Text("PassengersInjured"), new Text(columns[15]));
    		context.write(new Text("PassengersKilled"), new Text(columns[16]));
    		context.write(new Text("CyclistsInjured"), new Text(columns[17]));
    		context.write(new Text("CyclistsKilled"), new Text(columns[18]));
    		context.write(new Text("PedestriansInjured"), new Text(columns[19]));
    		context.write(new Text("PedestriansKilled"), new Text(columns[20]));
        }
        else {
          return;
        }
      }
  }
}