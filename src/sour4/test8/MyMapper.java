package sour4.test8;

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
            // 价格
            int price = Integer.parseInt(split[3]);
            // 销售数量
            int number = Integer.parseInt(split[7]);
            // 销售人员
            String saler = split[10];
            // 由于数据中销售人员存在空值，所以非空的数值才写入
            if (!saler.equals(""))
            // 将数据写入到context中
                context.write(new Text(saler),new Text(price+","+number));
        }
    }
}
