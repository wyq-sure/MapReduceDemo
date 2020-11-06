package sour4.test8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text,Text,Text, Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 需要将结果传递到context中的数据定义在循环外
        int sum = 0;
        // 将要判断的数据放在迭代器中进行判断，
        for (Text value: values) {
            String val = value.toString();
            String[] vals = val.split("\\,");
            int price = Integer.parseInt(vals[0]);
            int number = Integer.parseInt(vals[1]);
            sum+=price*number;
        }
        // 将用于分组的属性字段设置为key，然后将其他属性字段封装在value中，然后输出到文件中
        context.write(new Text("销售人员:"+key),new Text("销售额:"+sum));
    }
}
