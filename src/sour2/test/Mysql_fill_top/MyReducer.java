package sour2.test.Mysql_fill_top;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
    List<Double> list = new ArrayList<>();
    List<String> list2 = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        // 对传递过来的key值进行重新拆分
        String words = key.toString();
        String[] split = words.split("\\|");

        // 分别得到拆分之后的每一个值
        String  id = split[0];
        String  city = split[1];
        String job = split[2];
        double  salary = Double.parseDouble(split[3]);

        if (salary == 0.0){
            for (DoubleWritable val : values) {
                salary = val.get();
            }
        }


        // 将替换完的值与其他值一起重新做一次拼接
        String string = id+"|"+city+"|"+job+"|"+salary;

        context.write(new Text(string),NullWritable.get());
    }
}
