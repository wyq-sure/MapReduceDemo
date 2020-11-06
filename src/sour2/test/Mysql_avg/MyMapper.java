package sour2.test.Mysql_avg;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Pojo, Text, DoubleWritable> {
    @Override
    protected void map(Object key, Pojo value, Context context) throws IOException, InterruptedException {
        // 这里拿到的就是数据库中的一条数据，然后存在了一行中
        String words = value.toString();
        String[] string = words.split("\\|");
        String id = string[0].trim();
        String city = string[1].trim();
        String job  = string[2].trim();
        double salary = Double.parseDouble(string[3]);
        /*
        应用到其他的求平局值上，就是说可以先把判断出空值、离群值或者其他的需要统计其他的数据然后计算之后进行填充的，
        可以先同一替换成一个值，然后根据这个值传入不同的context，
        在reduce过程把值都取出来，然后进行计算，再把所有的值进行拼接，统一再写到context中。
         */
        String str = id+"|"+city+"|"+job;

        // 如果要统计指定指定城市、指定岗位的平均工资的话就多加一个判断
        if (job.equals("大数据"))
            context.write(new Text(city),new DoubleWritable(salary));
        else
            context.write(new Text(city),new DoubleWritable(0.0));
    }
}
