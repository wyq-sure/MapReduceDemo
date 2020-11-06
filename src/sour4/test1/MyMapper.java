package sour4.test1;

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
            // 年龄
            int age = Integer.parseInt(split[2]);
            // 如果需要统计不同会员卡的等级可以更改age
            context.write(new IntWritable(1),new IntWritable(age));
        }
    }
}
