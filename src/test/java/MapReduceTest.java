import bd.homework1.DataContainer;
import bd.homework1.HW1Mapper;
import bd.homework1.HW1Reducer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class MapReduceTest {
    private MapDriver<Object, Text, Text, DataContainer> mapDriver;
    private ReduceDriver<Text, DataContainer, Text, DataContainer>  reduceDriver;
    private MapReduceDriver<Object, Text, Text, DataContainer, Text, DataContainer> mapReduceDriver;
    private String testLine = "72.24.46.43 - - [06/Mar/2019:12:32:41 +0100] \"GET https://google.com\" 306 839 \"-\"Mozilla/6.0 (Windows; U; Windows NT 6.0; en-US) Gecko/2009032609 (KHTML, like Gecko) Chrome/2.0.172.6 Safari/530.7";

    @Before
    public void setUp(){
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = mapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(testLine))
                 .withOutput(new Text("72.24.46.43"), new DataContainer(new FloatWritable(0.0f), new IntWritable(839), new IntWritable(1)))
                 .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<DataContainer> values = new ArrayList<DataContainer>();
        values.add(new DataContainer(new FloatWritable(0.0f), new IntWritable(839), new IntWritable(1)));
        reduceDriver
                .withInput(new Text("72.24.46.43"), values)
                .withOutput(new Text("72.24.46.43"), new DataContainer(new FloatWritable(839.0f), new IntWritable(839), new IntWritable(1)))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testLine))
                .withInput(new LongWritable(), new Text(testLine))
                .withInput(new LongWritable(), new Text(testLine))
                .withOutput(new Text("72.24.46.43"), new DataContainer(new FloatWritable(839.0f), new IntWritable(839*3), new IntWritable(3)))
                .runTest();
    }

    @Test
    public void testCheckIP() throws IOException{
        assertTrue(HW1Mapper.checkIP("10.0.0.1"));
        assertTrue(HW1Mapper.checkIP("134.54.2.3"));
        assertFalse(HW1Mapper.checkIP("258.387.785.5"));
        assertFalse(HW1Mapper.checkIP("a1.b2.b3.b4"));
        assertFalse(HW1Mapper.checkIP("ad.75.1.1234"));
        assertFalse(HW1Mapper.checkIP("127.00.1"));
        assertFalse(HW1Mapper.checkIP("64644545"));
        assertFalse(HW1Mapper.checkIP("1d.83.167.55"));
        assertFalse(HW1Mapper.checkIP(".83.167.55"));
        assertFalse(HW1Mapper.checkIP("lk;jsadfn"));
        assertFalse(HW1Mapper.checkIP("***.***.***.***"));
        assertFalse(HW1Mapper.checkIP("********"));
    }
}
