package sour4.test2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class MyReducer extends Reducer<IntWritable,IntWritable,Text, NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        double sum_1 = 0;
        double sum_2 = 0;
        double sum_3 = 0;
        double sum_4 = 0;
        double sum_all = 0;
        double rate_1 = 0;
        double rate_2 = 0;
        double rate_3 = 0;
        double rate_4 = 0;
        /*
        错误整理：如果年龄放在key中，会输出多次的你在context中指定的字段
         */
        // 将要判断的数据放在迭代器中进行判断，
        for (IntWritable age : values) {
            if (age.get()<25){
                // 这里的1其实也可以用之前传递过来的key.get()去实现
                sum_1+=1;
            }else if (age.get()>=25 && age.get()<35){
                sum_2+=1;
            }else if(age.get()>=35 && age.get()<55){
                sum_3+=1;
            }else if (age.get()>=55 && age.get()<200){
                sum_4+=1;
            }else {
                System.out.println("未处理！");
            }
        }
        sum_all = sum_1 + sum_2 + sum_3+sum_4;
        rate_1 = sum_1/sum_all *100;
        rate_2 = sum_2/sum_all *100;
        rate_3 = sum_3/sum_all *100;
        rate_4 = sum_4/sum_all *100;
        DecimalFormat df = new DecimalFormat("0.00");
        context.write(new Text("25岁以下所占年龄比例:"+df.format(rate_1)+"%\n"
                +"25~35岁所占年龄比例:"+df.format(rate_2)+"%\n"
                +"35~55岁所占年龄比例:"+df.format(rate_3)+"%\n"
                +"55岁以上所占年龄比例:"+df.format(rate_4)+"%"),NullWritable.get());
    }

}
