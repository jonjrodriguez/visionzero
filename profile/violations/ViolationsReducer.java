import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ViolationsReducer
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  // Set of columns that contain String values
	  Set<String> stringColumns = new HashSet<String>();
	  stringColumns.add("Violation");
	  
	  int MaxStringLength = 0;
	  double min = 0;
	  double max = 0;
	  
	  for (Text value : values) {
		  int stringVal = 0;
		  double val = 0;
		  
		  // Check if key/columnName is a String or Int DataType
		  // Then compute MaxStringLength for String columns and Min/Max for Int Columns
		  if (stringColumns.contains(key.toString())) {
			  stringVal = value.toString().length();
			  
			  if (stringVal > MaxStringLength) {
				  MaxStringLength = stringVal;
			  }
		  }
		  else {
			  if (value.toString().equals(" ")) {
				  val = 0;
			  }
			  else {
				  val = Double.parseDouble(value.toString());
			  }
			  
			  if (val < min) {
				  min = val;
			  }
			  else if (val > max) {
				  max = val;
			  }
		  }
	  }
	  
	  if (stringColumns.contains(key.toString())) {
		  context.write(key, new Text("Max String Length: " + MaxStringLength));
	  }
	  else {
		  context.write(key, new Text("Min: " + min + " Max: " + max));
	  }
  }
}