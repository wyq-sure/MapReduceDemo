package sour2.test.Mysql_avg;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        // 用来格式化数据格式的，这里是保留两位小数，注意其返回值是String类型的，在之后传递的时候还需要转一下
        DecimalFormat df = new DecimalFormat( "0.00");
        // 保证每一个key值在计算时都初始化一下变量
        double sum = 0.0;
        int count = 0;
        double avg = 0.0;
        // 遍历当前key下的所有的属性值，进行计算
        for (DoubleWritable value : values) {
            sum += value.get();
            if (value.get() != 0.0)
                count++;
        }
        // 单独count=0的时候，说明当前城市没有对应岗位设置工资为0.0
        if (count != 0)
            avg = sum/count;
        else
            avg = 0.0;
        // 将数据写到Hdfs中，然后在指定精度的情况下
        context.write(new Text(key),new DoubleWritable(Double.parseDouble(df.format(avg))));
        // context.write(new Text(key),new DoubleWritable(Mysql_avg));
    }
}
