import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

public class TlcEtlMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
    private static final Logger _logger = Logger.getLogger(TlcEtlMapper.class.toString());
    private static final SimpleDateFormat _format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private int distance_col;
    private int fare_col;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        if (fileName.contains("green")) {
            distance_col = 10;
            fare_col = 11;
        } else {
            distance_col = 4;
            fare_col = 12;
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String val = value.toString();
        String[] record = val.split(",");

        if (skipRecord(record)) {
            return;
        }

        String formattedTime = "";
        String formattedMph = "";
        Double mph = 0.0;

        try {
            Date pickup = _format.parse(record[1]);
            Date dropoff = _format.parse(record[2]);

            Double distance = Double.parseDouble(record[distance_col]);
            long seconds = (dropoff.getTime() - pickup.getTime()) / 1000;
            mph = distance / (seconds / 3600.0);

            formattedTime = String.format("%.2f", seconds / 60.0);
            formattedMph = String.format("%.2f", mph);
        } catch (ParseException ignored) {}

        // Too slow or fast
        if (mph < 5.0 || mph > 80.0) {
            return;
        }

        // Too long
        if (seconds > 1.5 * 3600.0) {
            return;
        }

        // vendor_id, pickup, dropOff, trip_length, trip_distance, trip_mph, fare_amount
        String results = record[0] + "," + record[1] + "," + record[2] + "," + formattedTime + "," +
                record[distance_col] + "," + formattedMph + "," + record[fare_col];

        context.write(NullWritable.get(), new Text(results));
    }

    private boolean skipRecord(String[] record) {
        if (headerOrEmpty(record)) {
            return true;
        }

        if (!validPickup(record[1])) {
            return true;
        }

        if (!validDropOff(record[1], record[2])) {
            return true;
        }

        if (!validDistance(record[distance_col])) {
            // Only care about rides that 2 or more miles
            return true;
        }

        if (!validFare(record[fare_col], record[distance_col])) {
            // Only care about rides more than $5 and less than distance * $7
            return true;
        }

        return false;
    }

    private boolean headerOrEmpty(String[] record) {
        return record[0].equals("VendorID") || record.length == 1;
    }

    private boolean validPickup(String pickup) {
        Date date;
        Date minDate;

        try {
            date = _format.parse(pickup);
            minDate = _format.parse("2012-12-31 12:00:00");
        } catch (ParseException e) {
            return false;
        }

        return date.after(minDate);
    }

    private boolean validDropOff(String pickup, String dropOff) {
        Date pickupDate;
        Date dropOffDate;

        try {
            pickupDate = _format.parse(pickup);
            dropOffDate = _format.parse(dropOff);
        } catch (ParseException e) {
            return false;
        }

        return dropOffDate.after(pickupDate) && dropOffDate.before(new Date());
    }

    private boolean validDistance(String distance) {
        Double dist;

        try {
            dist = Double.parseDouble(distance);
        } catch (NumberFormatException e) {
            return false;
        }

        return !dist.isNaN() && dist >= 2.0;
    }

    private boolean validFare(String fare, String distance) {
        Double doubleFare;
        Double dist;

        try {
            doubleFare = Double.parseDouble(fare);
            dist = Double.parseDouble(distance);
        } catch (NumberFormatException e) {
            return false;
        }

        return !doubleFare.isNaN() && doubleFare >= 5.0 && doubleFare < (dist * 7);
    }
}
