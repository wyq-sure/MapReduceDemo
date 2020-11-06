package sour2.test.Mysql_sum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object,Pojo, Text, IntWritable> {
    @Override
    protected void map(Object key, Pojo value, Context context) throws IOException, InterruptedException {
        // 这里拿到的就是数据库中的一条数据，然后存在了一行中
        String words = value.toString();
        String[] string = words.split("\\|");
        String job  = string[8];
        context.write(new Text(job.trim()),new IntWritable(1));
    }
}
