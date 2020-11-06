package sour4.test3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class MyReducer extends Reducer<Text,Text,Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String words = key.toString();
        String[] words_split = words.split("\\,");
        String dept = words_split[0];
        String job = words_split[1];

        int count = 0;
        double sum = 0;
        double avg = 0;

        int count1 = 0;
        double sum1 = 0;
        double avg1 = 0;

        int count2 = 0;
        double sum2 = 0;
        double avg2 = 0;

        int count3 = 0;
        double sum3 = 0;
        double avg3 = 0;

        for (Text value : values) {
            String val = value.toString();
            String[] vals = val.split("\\,");
            int age = Integer.parseInt(vals[0]);
            double salary = Double.parseDouble(vals[1]);
            // 依次判断不同年龄下的工资和个数，计算出总数和平均工资
            if (age<25) {
                count++;
                sum+=salary;
                avg = sum/count;
            }else if (age>=25 && age<35){
                count1++;
                sum1+=salary;
                avg1 = sum1/count1;
            }else if (age>=35 && age<55){
                count2++;
                sum2+=salary;
                avg2 = sum2/count2;
            }else if (age>=55){
                count3++;
                sum3+=salary;
                avg3 = sum3/count3;
            }else {
                System.out.println("未处理！");
            }
        }

        // 然后将当前职位下的不同年龄段的平均工资显示
        DecimalFormat df = new DecimalFormat("0.00");
        context.write(new Text(dept+"-"+job), new Text("\n"+"25岁以下:" + df.format(avg)+"\n"
                +"25岁到35岁:" + df.format(avg1)+"\n"
                +"35岁到55岁:" + df.format(avg2)+"\n"
                +"55岁以上:" + df.format(avg3)));

    }
}

/*
        // 当前输出格式：
//        产品开发部-副经理
//        25岁以下:9286.00
//        25岁到35岁:12824.43
//        35岁到55岁:8757.85
//        55岁以上:0.00
        // 另外一种输出格式
        context.write(new Text(dept+"-"+job), new Text("25岁以下:" + df.format(avg)));
        context.write(new Text(dept+"-"+job), new Text("25岁到35岁:" + df.format(avg1)));
        context.write(new Text(dept+"-"+job), new Text("35岁到55岁:" + df.format(avg2)));
        context.write(new Text(dept+"-"+job), new Text("55岁以上:" + df.format(avg3)));
//        产品开发部-副经理	25岁以下:9286.00
//        产品开发部-副经理	25岁到35岁:12824.43
//        产品开发部-副经理	35岁到55岁:8757.85
//        产品开发部-副经理	55岁以上:0.00
*/
