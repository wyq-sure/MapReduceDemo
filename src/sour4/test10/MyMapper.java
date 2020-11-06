package sour4.test10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String words = value.toString();

        // 读取csv文件的时候不读第一行数据
        if (key.toString().equals("0")){
            System.out.println(words);
        }else {
            // 对剩下的数据进行操作
            String[] split = words.split("\\,");
            // 销售数量
            int number = Integer.parseInt(split[7]);
            // 销售地区
            String sale_source = split[9];
            String store_name = split[12];
            if (store_name.equals("连锁"))
                 // 将数据写入到context中
                context.write(new Text(sale_source),new IntWritable(number));
        }
    }
}
