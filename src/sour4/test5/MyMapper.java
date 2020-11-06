package sour4.test5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, IntWritable,Text> {
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
            // 会员等级
            int grade = Integer.parseInt(split[5]);
            // 将年龄和会员等级封装到在value中，这样
            context.write(new IntWritable(1),new Text(age+","+grade));
        }
    }
}
