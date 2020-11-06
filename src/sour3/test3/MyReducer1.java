package sour3.test3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer1 extends Reducer<Text, Text,Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 这里的key设置的是两个：省份和酒店名
        long sum_pre = 0;
        long sum_com = 0;
        String status;
        for (Text value : values) {
            status = value.toString();
            // 根据订单状态进行判断的：这里只是判断的预定和完成的，还可以根据状态去判断其他的拒单、切单的
            if (status.equals("0")){
                sum_pre+=1;
            }else {
                sum_com+=1;
            }
        }
        // 根据指定的key分组（他会把所有相同的key聚合在一起，然后统计），然后酒店数量和房间数量
        context.write(new Text(key), new Text("酒店预定数量:"+sum_pre+";"+"订单完成数量:"+sum_com));
    }
}
