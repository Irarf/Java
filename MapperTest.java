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


    private final String testIP = "4,1617136468619,49";


    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withOutput(new HW1Writable(new Text("Node4SSD"),new Text("1617136468619")), new IntWritable(49))
                .runTest();
    }

}