
import laba1.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapperTest {

    private MapDriver<LongWritable, Text, HW1Writable, IntWritable> mapDriver;
    private ReduceDriver<HW1Writable, IntWritable, Text,IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, HW1Writable, IntWritable, Text, IntWritable> mapReduceDriver;

    private final String testIP = "4,1617136468619,49";


    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withOutput(new HW1Writable(new Text("4"),new Text("1617136468619")), new IntWritable(49))
                .runTest();
    }
    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(49));
       // values.add(new IntWritable(1));
        reduceDriver
                .withInput(new HW1Writable(new Text("4"),new Text("1617136468000")), values)
                .withOutput(new Text("4,1617136468000,nullnull"), new IntWritable(49))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withOutput(new Text("4,1617136468000,nullnull"), new IntWritable(49))
                .runTest();
    }
}
