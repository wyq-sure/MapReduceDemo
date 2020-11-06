package sour3.test1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer1 extends Reducer<Text,Text,Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 这里的key设置的是两个：省份和酒店名
        long sum_hotel = 0;
        long sum_room = 0;
        // value中包括的是：房间数和酒店数1
        for (Text value : values) {
            String string = value.toString();
            String[] split = string.split("\\,");
            long count_room = Long.parseLong(split[0]);
            long count = Long.parseLong(split[1]);
            sum_hotel+=count;
            sum_room+=count_room;
        }
        // 根据指定的key分组（他会把所有相同的key聚合在一起，然后统计），然后酒店数量和房间数量
        context.write(new Text(key), new Text(sum_hotel+","+sum_room));
    }
}
