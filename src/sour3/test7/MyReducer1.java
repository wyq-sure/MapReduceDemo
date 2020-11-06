package sour3.test7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class MyReducer1 extends Reducer<Text, Text,Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 这里的key设置的是两个：省份和酒店名
        double sum_all = 0;
        double sum_all_temp = 0;
        double sum_com = 0;
        double rate = 0;
        String words;
        for (Text value : values) {
            words = value.toString();
            String[] split = words.split("\\,");
            int count_room = Integer.parseInt(split[0]);
            String status = split[1];
            // 判断订单状态是否是完成的
            if (status.equals("1")){
                sum_com += count_room;
            }else { // 如果需要根据其他状态信息去统计相应的订单信息，只需要在这里加一些判断
                sum_all_temp += count_room;
            }
        }
        sum_all = sum_all_temp + sum_com;
        rate = sum_com / sum_all*100;
        // 用来格式化数据格式的，这里是保留两位小数，注意其返回值是String类型的，在之后传递的时候还需要转一下
        DecimalFormat df = new DecimalFormat( "0.00");
        // 根据指定的key分组（他会把所有相同的key聚合在一起，然后统计），然后酒店数量和房间数量
        context.write(new Text(key), new Text("客房出租率：:"+df.format(rate)+"%"));
    }
}
