package bd.homework1;
import java.io.*;
import org.apache.hadoop.io.*;

public class DataContainer implements Writable {

    private IntWritable totalBytes;
    private IntWritable requests;
    private FloatWritable averageBytes;

    public DataContainer(){
        this.averageBytes = new FloatWritable(0.0f);
        this.totalBytes = new IntWritable(0);
        this.requests = new IntWritable(0);
    }

    public DataContainer(FloatWritable average, IntWritable totalBytes, IntWritable requests){
        this.averageBytes = average;
        this.totalBytes = totalBytes;
        this.requests = requests;
    }

    public FloatWritable getAverageBytes(){
        return this.averageBytes;
    }

    public IntWritable getTotalBytes(){
        return this.totalBytes;
    }

    public IntWritable getRequest(){
        return this.requests;
    }

    @Override
    public void write(DataOutput dataOut) throws IOException {
        totalBytes.write(dataOut);
        requests.write(dataOut);
        averageBytes.write(dataOut);
    }

    @Override
    public void readFields(DataInput dataIn) throws IOException {
        totalBytes.readFields(dataIn);
        requests.readFields(dataIn);
        averageBytes.readFields(dataIn);
    }

    @Override
    public int hashCode(){
        return totalBytes.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof DataContainer) {
            DataContainer dc = (DataContainer) object;
            return  totalBytes.equals(dc.totalBytes) &&
                    requests.equals(dc.requests) &&
                    averageBytes.equals(dc.averageBytes);
        }
        return false;
    }

    @Override
    public String toString(){
        return totalBytes.toString() + ";" + averageBytes.toString();
    }

    public void set(IntWritable bytes, IntWritable requests, FloatWritable average) {
        this.averageBytes = average;
        this.totalBytes = bytes;
        this.requests = requests;
    }
}
