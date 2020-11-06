package sour3.test5;

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
            // 统计不同地区的订单数量
            String name = split[1];
            // 酒店的类型
            String type = split[10];
            // 酒店所在的省份
            String province = split[2];
            // 酒店所在的市
            String city = split[3];
            // 酒店的相关信息
            String grade = split[7]; // 评分
            String comment = split[8]; // 评论数
            String count_room = split[5];// 客房数
            if (type.equals("高端"))
            // 不同省份地区的酒店名作为key;然后评分、评论数、客房数作为value
                context.write(new Text(province+"-"+city+","+name),new Text("评分:"+grade+","+"评论数:"+comment+","+"客房数:"+count_room));
        }
    }
}
