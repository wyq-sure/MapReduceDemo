package sour4.test10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, IntWritable,Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 酒店数量计数
        int sum = 0;
        /*
        错误警示：如果在循环中写，那么就是每个连锁酒店都对应一个城市；
                  而在循环之外写呢，MR会自动的根据key的值去封装，
                  然后根据每个地区，计算出当前地区的所有酒店的数据
                  Mapper得到的格式是：<地区:数量1，数量2，数量3.....>
                  Reducer进行计算的时候就是：根据每一个key对应的地区，计算出当前key对应的数量的和，
                  也就是当前地区的所有的酒店的数量，
                  另外在传到Reducer的时候，已经控制了只有连锁酒店才传递过来，再进行计算！
         */
        for (IntWritable value: values) {
            // 依次循环每一个key对应的所有的值，然后相加
           sum+=value.get();
        }
        // 将用于分组的属性字段设置为key，然后将其他属性字段封装在value中，然后输出到文件中
        context.write(new Text("地区:"+key),new Text("连锁酒店的销售数量:"+sum));
    }
}
