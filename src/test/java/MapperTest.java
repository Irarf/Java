
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import laba1.*;

import org.apache.hadoop.mrunit.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;


public class MapperTest {

    private MapDriver<LongWritable, Text, HW1Writable, IntWritable> mapDriver;

    @BeforeAll
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("1,113123123123,3"));
        mapDriver.withOutput(new HW1Writable(new Text("1"),new Text("10130130103")), new IntWritable(3));
        mapDriver  .runTest();
    }


}
