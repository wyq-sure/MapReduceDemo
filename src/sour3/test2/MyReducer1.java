package sour3.test2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer1 extends Reducer<Text, LongWritable,Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // 这里的key设置的是两个：省份和酒店名
        long sum = 0;
        // value中包括的是：当前订单数量
        for (LongWritable value : values) {
            sum+=value.get();
        }
        // 根据指定的key分组（他会把所有相同的key聚合在一起，然后统计），总的订单数量
        context.write(new Text(key), new LongWritable(sum));
    }
}
