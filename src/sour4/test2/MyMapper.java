package sour4.test2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, IntWritable,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String words = value.toString();
        // 读取csv文件的时候不读第一行数据
        if (key.toString().equals("0")){
            System.out.println(words);
        }else {
            // 对剩下的数据进行操作
            String[] split = words.split("\\,");
            int age = Integer.parseInt(split[3]);
            context.write(new IntWritable(1),new IntWritable(age));
        }
    }
}
