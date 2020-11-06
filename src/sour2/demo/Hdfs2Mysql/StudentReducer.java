package sour2.demo.Hdfs2Mysql;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public  class StudentReducer extends Reducer<LongWritable, Text, Pojo, NullWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        // 依次遍历每一行传递过来的值
        for(Text text : values){
            // 这样拿到的是每一行的文本
            String string = text.toString();
            // 每一行的文本是按制表符分割的
            String[] studentArr = string.split("\t");
            // 根据位置逐个赋值给相应的变量
            String name = studentArr[0].trim();
            int age = Integer.parseInt(studentArr[1].trim());
            // 通过构造方法传到实体类的对象中
            context.write(new Pojo(name, age), NullWritable.get());
        }
    }
}