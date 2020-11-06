package sour3.test1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper2 extends Mapper<LongWritable, Text,Text, Text> {
    /**
     * 第二个MR读到的是："省份,酒店名 房间数,酒店数"
     * 以上的数据是封装在一个Text中
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 先将数据转成字符串类型
        String words = value.toString();
        // 把数据先按key和value进行分割
        String[] split_words  = words.split("\t");
        String MR_key = split_words[0];
        String MR_value = split_words[1];
        // 把key和value进行分割
        String[] split_key = MR_key.split("\\,");
        String[] split_value = MR_value.split("\\,");
        // 从key中取出省份
        String province = split_key[0];
        // 从value取出房间数和酒店数
        String sum_hotel = split_value[0];
        String sum_room = split_value[1];
        // 然后根据省份分组，所以key中只是设置省份一个属性，然后其他的传递数据
        context.write(new Text(province), new Text(sum_hotel+","+sum_room));
    }
}
