package laba1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class HW1Reducer
        extends Reducer<HW1Writable, IntWritable, Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private Text Id = new Text();
    private Text St = new Text();

    /**
     *
     * @param key Input. Custom type key(Id, date)
     * @param values Input. Values
     * @param context Output (Text, int)
     */
    public void reduce(HW1Writable key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        String interval = context.getConfiguration().get("interval");
        String intervalCount = context.getConfiguration().get("intervalCount");
        int sum = 0;
        String test;

        Id = key.getmetricId();
        for (IntWritable val :values) {
            sum +=val.get();

        }

        St = key.getTimestamp();
        test = Id.toString();
        test += ",";
        test = test.concat(St.toString());
        test += "," +intervalCount + interval;
        result.set(sum);
        context.write(new Text(test), result);

    }
}
