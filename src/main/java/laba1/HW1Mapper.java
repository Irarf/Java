package laba1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Mapper class
 */

public class HW1Mapper
        extends Mapper<LongWritable, Text, HW1Writable, IntWritable> {



    private HW1Writable hwLog = new HW1Writable();

    private IntWritable value = new IntWritable();
    private Text timestamp = new Text();
    private Text metricId = new Text();

     /**
     *
     * @param key Input key
     * @param values Input String
     * @param context Output result (Custom type, IntWritable)

     */

    @Override
    public void map(LongWritable key, Text values, Context context
    ) throws IOException, InterruptedException {

        String interval = context.getConfiguration().get("interval");
        String intervalCount = context.getConfiguration().get("intervalCount");

        long intC =Long.parseLong(intervalCount);

        ArrayList<String> lines = new ArrayList<>();
        try(Scanner scan = new Scanner(new File("/root/laba1/bible"))){
            while(scan.hasNextLine()){
                lines.add(scan.nextLine());
            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }

        String[] array = lines.toArray(new String[0]);
        long tmp = 0;
        String[] words = values.toString().split(",");

        if ("1".equals(words[0])) {
            metricId = new Text(array[1]);
        } else if ("2".equals(words[0])) {
            metricId = new Text(array[2]);
        } else if ("3".equals(words[0])) {
            metricId = new Text(array[3]);
        } else if ("4".equals(words[0])) {
            metricId = new Text(array[4]);
        } else if ("5".equals(words[0])) {
            metricId = new Text(array[5]);
        } else if ("6".equals(words[0])) {
            metricId = new Text(array[6]);
        } else if ("7".equals(words[0])) {
            metricId = new Text(array[7]);
        } else if ("8".equals(words[0])) {
            metricId = new Text(array[8]);
        } else if ("9".equals(words[0])) {
            metricId = new Text(array[9]);
        } else {
            metricId = new Text(array[0]);
        }

        timestamp.set(words[1]);
        tmp = Long.parseUnsignedLong(String.valueOf(timestamp));

        if ("s".equals(interval)){
            tmp = tmp - (tmp % (1000 * intC));
        }else if ("m".equals(interval)){
            tmp = tmp - (tmp % (60000 * intC));
        }else if ("h".equals(interval)){
            tmp = tmp - (tmp % (3600000 * intC));
        }

        String strLong = Long.toString(tmp);
        timestamp.set(strLong);
        String str;
        str=metricId.toString();

        if ("error".equals(str)){
            value.set(1);
        }else{
            value.set(Integer.parseInt(words[2]));
        }

        hwLog.set(metricId,timestamp);
        context.write(hwLog, value);

    }
}