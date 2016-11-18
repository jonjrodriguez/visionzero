import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TlcProfileMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");

        if (columns[0].equals("VendorID") || columns.length == 1) {
            return;
        }

        int distance_col = 4;
        int fare_col = 12;

        if (columns.length == 21) {
            distance_col = 10;
            fare_col = 11;
        }

        // pickup
        context.write(new Text("tpep_pickup_datetime"), new Text(columns[1]));

        // dropoff
        context.write(new Text("tpep_dropoff_datetime"), new Text(columns[2]));

        // distance
        context.write(new Text("trip_distance"), new Text(columns[distance_col]));

        // fare
        context.write(new Text("fare_amount"), new Text(columns[fare_col]));

        // mph
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date pickup = format.parse(columns[1]);
            Date dropoff = format.parse(columns[2]);

            double distance = Double.parseDouble(columns[distance_col]);
            long seconds = (dropoff.getTime() - pickup.getTime()) / 1000;
            double mph = (distance > 0 && seconds > 0) ? distance / (seconds / 3600.0) : 0;

            context.write(new Text("calculated_time_minutes"), new Text(String.format("%.2f", seconds / 60.0)));
            context.write(new Text("calculated_average_mph"), new Text(String.format("%.2f", mph)));
        } catch (ParseException ignored) {
            return;
        }
    }
}
