package bd.homework1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;

/**
 * Вход: строка из лога
 * Парсит строку, берет IP-адрес и количество байт
 * bytes это количество полученных байт
 * параметры requests и averageBytes передается 1
 */

public class HW1Mapper extends Mapper<Object, Text, Text, DataContainer> {

    private Text ip = new Text();
    private IntWritable bytes = new IntWritable(0);
    private IntWritable requests = new IntWritable(1);
    private FloatWritable averageBytes = new FloatWritable(0.0f);
    private DataContainer dataContainer = new DataContainer();

    private static boolean convertToIntCheck(String strNum) {
        try {
            int digit = Integer.parseInt(strNum);
        } catch (NumberFormatException | NullPointerException npe) {
            return false;
        }
        return true;
    }

    public static boolean checkIP(String ip){
        boolean result = true;
        int octa = 0;
        String[] parsedIP = ip.split("\\.");
        result = (parsedIP.length == 4);
        for(String i: parsedIP){
            if (convertToIntCheck(i)) {
                octa = Integer.parseInt(i);
                result = result && octa >= 0 && octa <= 255;
            }
            else {
                result = false;
            }
        }
        return result;
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parsedLine = line.split(" ");
        try {
            String IP = parsedLine[0];
            String bytesString = parsedLine[8];
            if(checkIP(IP) && convertToIntCheck(bytesString)) {
                bytes.set(Integer.parseInt(bytesString));
                requests.set(1);
                ip.set(parsedLine[0]);
                dataContainer.set(bytes, requests, averageBytes);
                context.write(ip, dataContainer);
            }
        }
        catch (Exception exception) {
            System.out.println("ERROR: " + exception.getMessage());
        }
    }
}