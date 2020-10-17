package bd.homework1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 1. Производит сумму всех байт полученных от Mapper, и выдаёт суммарное количество байт
 * 2. Производит сумму количества запросов, полученных от Mapper - суммарное кол-во запросов, полученное с определенного IP
 * 3. Производит расчет среднего знаечния полученных байт, по определенному IP
 */

public class HW1Reducer extends Reducer<Text, DataContainer, Text, DataContainer> {
    private IntWritable writableBytes = new IntWritable();
    private IntWritable writableRequests = new IntWritable();
    private FloatWritable writableAverageBytes = new FloatWritable();
    private DataContainer dataContainer = new DataContainer();

    @Override
    public void reduce(Text key, Iterable<DataContainer> values, Context context) throws IOException, InterruptedException {
        int totalBytes = 0;
        int requests = 0;

        for(DataContainer item : values){
            totalBytes += item.getTotalBytes().get();
            requests += item.getRequest().get();
        }
        float averageBytes = 0;

        if(requests != 0) {
            averageBytes = (float) totalBytes / requests;
        }
        else{
            averageBytes = 0.0f;
        }

        writableBytes.set(totalBytes);
        writableRequests.set(requests);
        writableAverageBytes.set(averageBytes);

        dataContainer.set(writableBytes, writableRequests, writableAverageBytes);
        context.write(key, dataContainer);
    }
}
