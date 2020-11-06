package sour4.test7;

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
            // 取出促销状态
            int promotion = Integer.parseInt(split[8]);
            // 价格
            int price = Integer.parseInt(split[3]);
            // 销售数量
            int number = Integer.parseInt(split[7]);
            // 商品分类
            String classify = split[5];
            // 是否是季节性商品
            String season = split[4];
            // 将数据写入到context中
            context.write(new Text(classify),new Text(promotion+","+price+","+number+","+season));
        }
    }
}
