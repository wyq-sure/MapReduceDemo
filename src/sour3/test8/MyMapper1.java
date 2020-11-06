package sour3.test8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper1 extends Mapper<LongWritable, Text,Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 先将数据转成字符串类型
        String words = value.toString();
        // map的输入key为当前行在文件内的位置偏移量，所以首行的偏移量肯定是0，
        // 那么我们可以选择第一行不做处理，只是打印当前行的内容
        if (key.toString().equals("0")) {
           System.out.println(words);
        } else {
            // 按csv格式的指定的分隔符进行分割
            String[] split = words.split("\\,");
            // 统计不同地区的订单数量
            String name = split[1];
            // 省份
            String province = split[2];
            // 城市
            String city = split[3];
            // 订单状态
            String order_status = split[13];
            // 统计满足“分销”状态的酒店订单信息
            if (order_status.equals("2"))
                context.write(new Text(province+","+city + "," + name),new LongWritable(1));
        }
    }
}
