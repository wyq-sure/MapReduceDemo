package sour3.test7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper1 extends Mapper<LongWritable, Text,Text, Text> {
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
            // 酒店名称
            String hotel = split[1];
            // 客房数
            String count_room = split[5];
            // 拿到酒店的类型
            String type = split[10];
            // 订单状态
            String order_status = split[13];
            // 酒店所在的省份
            String province = split[2];
            // 酒店所在的市
            String city = split[3];
            // 如果是要指定某个连锁酒店：可以指定一个&&hotel="某个连锁酒店的名字"
            if (type.equals("连锁")) {
                // 酒店名称作为key，酒店数目和酒店状态
                context.write(new Text(province + "," + city + "," + hotel), new Text(count_room + "," + order_status));
            }
        }
    }
}
