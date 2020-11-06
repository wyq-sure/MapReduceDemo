package sour4.test6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class MyReducer extends Reducer<IntWritable,Text,Text, NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum_un =0;
        int sum = 0;
        /*
        错误整理：如果年龄放在key中，会输出多次的你在context中指定的字段
         */
        // 将要判断的数据放在迭代器中进行判断，
        for (Text value: values) {
            String val = value.toString();
            String[] vals = val.split("\\,");
            int promotion = Integer.parseInt(vals[0]);
            int price = Integer.parseInt(vals[1]);
            int number = Integer.parseInt(vals[2]);

            if (promotion==0){
                sum_un+=price*number;
            }else {
                sum+=price*number;
            }
        }
        context.write(new Text("未促销时的销售额:"+sum_un+"\n"
                +"促销时的销售额:"+sum),NullWritable.get());
    }

}
