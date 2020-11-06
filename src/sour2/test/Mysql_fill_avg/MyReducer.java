package sour2.test.Mysql_fill_avg;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            // 对传递过来的key值进行重新拆分
            String words = key.toString();
            String[] split = words.split("\\|");

            // 分别得到拆分之后的每一个值
            String id = split[0];
            String city = split[1];
            String job = split[2];
            double salary = Double.parseDouble(split[3]);

       /*
         当前values中封装的是所有的平均值
         即时是每个有关大数据的平均值都计算了，但是只有判断出salary为0.0的时候才进行替换，
         其他时候的salary值不变
         而且这个salary是通过截取传过来的key而得到的
         */
            if (salary == 0.0){
                for (DoubleWritable val : values) {
                    salary = val.get();
                }
            }
            // 将替换完的值与其他值一起重新做一次拼接
            String string = id+"|"+city+"|"+job+"|"+salary;
            // 将拼接好的字符串写到context中
            context.write(new Text(string),NullWritable.get());
        }
    }

