package sour3.test1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer2 extends Reducer<Text,Text,Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long sum_hotel = 0;
        long sum_room = 0;
        // 根据省份分组之后，values是以迭代器的形式存储的，所以遍历迭代器进行取值
        for (Text value : values) {
            String string = value.toString();
            String[] split = string.split("\\,");
            // 对value的数据进行分割，取出每个省份对应的酒店数量和房间数量
            long count_hotel = Long.parseLong(split[0]);
            long count_room = Long.parseLong(split[1]);
            sum_hotel += count_hotel;
            sum_room += count_room;
        }
        // 第二次分组的时候只是根据省份进行分组
        context.write(new Text(key), new Text(sum_hotel+","+sum_room));
    }
}

/*
  @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long sum_hotel = 0;
        long sum_room = 0;
        // 根据省份分组之后，values是以迭代器的形式存储的，所以遍历迭代器进行取值
        for (Text value : values) {
            String string = value.toString();
            String[] split = string.split("\\,");
            // 对value的数据进行分割，取出每个省份对应的酒店数量和房间数量
            long count_hotel = Long.parseLong(split[0]);
            long count_room = Long.parseLong(split[1]);
            sum_hotel += count_hotel;
            sum_room += count_room;
        }
        // 第二次分组的时候只是根据省份进行分组
        context.write(new Text(key), new Text(sum_hotel+","+sum_room));
    }
 */