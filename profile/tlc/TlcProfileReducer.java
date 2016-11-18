import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TlcProfileReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        String results = "";
        if (key.toString().contains("datetime")) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date maxDate = new Date(Long.MIN_VALUE);
            Date minDate = new Date(Long.MAX_VALUE);
            boolean allValid = true;

            for (Text value : values) {
                try {
                    Date date = format.parse(value.toString());

                    maxDate = date.after(maxDate) ? date : maxDate;
                    minDate = date.before(minDate) ? date : minDate;;
                } catch (ParseException e) {
                    allValid = false;
                }
            }

            results = String.format("max: %s; min: %s; all_valid_dates: %s", format.format(maxDate), format.format(minDate), allValid);
        } else {
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            boolean allValid = true;

            for (Text value : values) {
                try {
                    double val = Double.parseDouble(value.toString());

                    max = val > max ? val : max;
                    min = val < min ? val : min;
                } catch (NumberFormatException e) {
                    allValid = false;
                }
            }

            results = String.format("max: %.02f; min: %.02f; all_valid_numbers: %s", max, min, allValid);
        }

        context.write(key, new Text(results));
    }
}
