package sour4.test5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<IntWritable,Text,Text, NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 分别为每个年龄段进行分组，统计会员等级的数量
        int sum_A0 = 0;
        int sum_A1 = 0;
        int sum_A2 = 0;
        int sum_A3 = 0;
        int sum_A4 = 0;
        int sum_A5 = 0;

        int sum_B0 = 0;
        int sum_B1 = 0;
        int sum_B2 = 0;
        int sum_B3 = 0;
        int sum_B4 = 0;
        int sum_B5 = 0;

        int sum_C0 = 0;
        int sum_C1 = 0;
        int sum_C2 = 0;
        int sum_C3 = 0;
        int sum_C4 = 0;
        int sum_C5 = 0;
        /*
        错误整理：如果年龄放在key中，会输出多次的你在context中指定的字段
         */
        // 将要判断的数据放在迭代器中进行判断
        for (Text value: values) {
            String val = value.toString();
            String[] vals = val.split("\\,");
            int age = Integer.parseInt(vals[0]);
            int grade = Integer.parseInt(vals[1]);
            // 根据年龄进行分组
            if (age>=20 && age<25){
                // 这里的1其实也可以用之前传递过来的key.get()去实现
                // 根据等级的不同，不同的计数器加1
                switch (grade){
                    case 0:
                        sum_A0+=1;
                        break;
                    case 1:
                        sum_A1+=1;
                        break;
                    case 2:
                        sum_A2+=1;
                        break;
                    case 3:
                        sum_A3+=1;
                        break;
                    case 4:
                        sum_A4+=1;
                        break;
                    case 5:
                        sum_A5+=1;
                        break;
                }
            }else if (age>=25 && age<35){
                // 根据等级的不同，不同的计数器加1
                switch (grade){
                    case 0:
                        sum_B0+=1;
                        break;
                    case 1:
                        sum_B1+=1;
                        break;
                    case 2:
                        sum_B2+=1;
                        break;
                    case 3:
                        sum_B3+=1;
                        break;
                    case 4:
                        sum_B4+=1;
                        break;
                    case 5:
                        sum_B5+=1;
                        break;
                }
            }else if(age>=35 && age<200){
                // 根据等级的不同，不同的计数器加1
                switch (grade){
                    case 0:
                        sum_C0+=1;
                        break;
                    case 1:
                        sum_C1+=1;
                        break;
                    case 2:
                        sum_C2+=1;
                        break;
                    case 3:
                        sum_C3+=1;
                        break;
                    case 4:
                        sum_C4+=1;
                        break;
                    case 5:
                        sum_C5+=1;
                        break;
                }
            }else {
                System.out.println("未处理！");
            }
        }
        // 将值都封装在一个属性中
        context.write(new Text("20~25岁会员分布情况:"+"0级会员:"+sum_A0+","+"1级会员:"+sum_A1+","+"2级会员:"+sum_A2+","+"3级会员:"+sum_A3+","+"4级会员:"+sum_A4+","+"5级会员:"+sum_A5+"\n"
             +"25岁~35岁会员分布情况:"+"0级会员:"+sum_B0+","+"1级会员:"+sum_B1+","+"2级会员:"+sum_B2+","+"3级会员:"+sum_B3+","+"4级会员:"+sum_B4+","+"5级会员:"+sum_B5+"\n"
                +"35岁以上会员分布情况:"+"0级会员:"+sum_C0+","+"1级会员:"+sum_C1+","+"2级会员:"+sum_C2+","+"3级会员:"+sum_C3+","+"4级会员:"+sum_C4+","+"5级会员:"+sum_C5),
                NullWritable.get());
    }
}
