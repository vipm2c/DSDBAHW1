package bd.homework1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ";"); // CSV format
        //int reducers_count = 1;
        Job job = Job.getInstance(conf, "Count of requests and average bytes");

        job.setJarByClass(DataContainer.class); // Основной класс-контейнер для данных
        job.setMapperClass(HW1Mapper.class); // Маппер
        job.setReducerClass(HW1Reducer.class); // Редуктор

        //job.setNumReduceTasks(reducers_count);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataContainer.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Задаем входную и выходную директории
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
