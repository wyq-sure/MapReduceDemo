package sour4.test4;

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

        int count_M1 = 0;
        int count_W1 = 0;
        int sum_1 = 0;
        double rate_1 = 0;

        int count_M2 = 0;
        int count_W2 = 0;
        int sum_2 = 0;
        double rate_2 = 0;

        int count_M3 = 0;
        int count_W3 = 0;
        int sum_3 = 0;
        double rate_3 = 0;

        int count_M4 = 0;
        int count_W4 = 0;
        int sum_4 = 0;
        double rate_4 = 0;

        for (Text value : values) {
            String val = value.toString();
            String[] vals = val.split("\\,");
            int age = Integer.parseInt(vals[0]);
            String gender = vals[1];

            if (age<25) {
                if (gender.equals("男"))
                    count_M1++;
                else
                    count_W1++;
                sum_1 = count_M1+count_W1;

            }else if (age>=25 && age<35){
                if (gender.equals("男"))
                    count_M2++;
                else
                    count_W2++;
                sum_2 = count_M2+count_W2;

            }else if (age>=35 && age<55){
                if (gender.equals("男"))
                    count_M3++;
                else
                    count_W3++;
                sum_3 = count_M3+count_W3;

            }else if (age>=55){
                if (gender.equals("男"))
                    count_M4++;
                else
                    count_W4++;
                sum_4 = count_M4+count_W4;

            }else {
                System.out.println("未处理！");
            }
        }

        // 然后将当前职位下的不同年龄段的男女个数显示
        context.write(new Text(dept+"-"+job), new Text("\n"+"25岁以下:" +"男:"+count_M1+","+"女:"+count_W1 +"\n"
                +"25岁到35岁:" + "男:"+count_M2+","+"女:"+count_W2+"\n"
                +"35岁到55岁:" + "男:"+count_M3+","+"女:"+count_W3+"\n"
                +"55岁以上:" +"男:"+count_M4+","+"女:"+count_W4 ));
    }
}


