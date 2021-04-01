package laba1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom type
 */
public class HW1Writable implements WritableComparable<HW1Writable>
{
    private Text metricId;
    private Text timestamp;

    public HW1Writable()
    {
        this.metricId = new Text();
        this.timestamp = new Text();

    }

    public HW1Writable(Text metric, Text ts)
    {
        this.metricId = metric;
        this.timestamp = ts;

    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        metricId.write(out);
        timestamp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        metricId.readFields(in);
        timestamp.readFields(in);

    }

    public Text getmetricId()
    {
        return metricId;
    }
    public Text getTimestamp()
    {
        return timestamp;
    }

    public void set(Text metricId, Text timestamp)
    {
        this.metricId = metricId;
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(HW1Writable o) {
        if (metricId.compareTo(o.metricId) == 0){
            return (timestamp.compareTo(o.timestamp));
        }
        else return (metricId.compareTo(o.metricId));
    }
    @Override
    public boolean equals(Object o)
    {
        if (o instanceof HW1Writable){
            HW1Writable other = (HW1Writable) o;
            return metricId.equals(other.metricId) && timestamp.equals(other.timestamp);
        }
        return false;
    }
    @Override
    public int hashCode()
    {
        return metricId.hashCode();
    }
}

