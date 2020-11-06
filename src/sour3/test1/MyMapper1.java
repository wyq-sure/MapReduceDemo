package sour3.test1;

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
        // 那么我们可以选择第一行不做处理，只是打印当前行的内容。
        if (key.toString().equals("0")) {
           System.out.println(words);
        } else {
            // 按csv格式的指定的分隔符进行分割
            String[] split = words.split("\\,");
            // 取出酒店名，然后统计酒店数量
            String name = split[1];
            String province = split[2];
            String count_room = split[5];
            // 控制key有两个属性来决定，value的属性值也可以传递多个
            // 然后根据两个属性进行分组
            context.write(new Text(province + "," + name), new Text(count_room + "," + 1));
        }
    }
}
