package sour2.test.Mysql_skill;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyMapper extends Mapper<Object, sourcePojo, Text, IntWritable> {
    @Override
    protected void map(Object key, sourcePojo value, Context context) throws IOException, InterruptedException {
        // 这里拿到的就是数据库中的一条数据，然后存在了一行中
        String words = value.toString();
        // 这是指定分隔符对不同的数据进行分割
        String[] string = words.split("\\|");
//        string.length = 12
//        System.out.println(string.length);

        /*
        我可以在这里之前就把岗位确定下来，然后传递的只是当前岗位的所有技能点，
        然后我可以在Reducer中对数据进行统计，
        或者在cleanup方法中进行求解也行，然后在Reducer过程中对数据的占比情况做一下统计。
        */
        String jobName = string[8];
        // 指定岗位名称为“大数据”的
        if (jobName.contains("大数据")) {
            // 岗位技能点
            String skills = string[7];
            // 使用正则表达式匹配你想要的数据
            Pattern pattern = Pattern.compile("\'(.*?)\'"); // 利用Pattern对象指定正则
            Matcher matcher = pattern.matcher(skills); // 利用Matcher进行匹配
            // 循环依次去取数据，所取到的数据就是你要取到的数据
            while (matcher.find()) {
                // 根据标识符将一个一个筛出来的数据放到list中
                String temp = matcher.group();
                // 对数据中不符合规范的进行清洗
                temp = temp.replace("\'", "");
                // 对全角与半角字符的处理
                temp = temp.replace("．", ".");
                // 在循环汇总依次将处理过之后的数据，并设置每一个的值是1
                context.write(new Text(temp), new IntWritable(1));
            }
        }
    }
}
