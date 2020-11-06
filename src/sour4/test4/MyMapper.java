package sour4.test4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String words = value.toString();
        // 读取csv文件的时候不读第一行数据
        if (key.toString().equals("0")){
            System.out.println(words);
        }else {
            // 对剩下的数据进行操作
            String[] split = words.split("\\,");
            String gender = split[2];
            // 年龄
            int age = Integer.parseInt(split[3]);
            // 所在的部门
            String dept = split[6];
            // 所在的部门的职位
            String job = split[7];
            context.write(new Text(dept+","+job),new Text(age+","+gender));
        }
    }
}
