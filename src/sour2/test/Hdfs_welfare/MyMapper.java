package sour2.test.Hdfs_welfare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyMapper extends Mapper<Object, Pojo, Text, Text> {
    @Override
    protected void map(Object key, Pojo value, Context context) throws IOException, InterruptedException {
        // 这里拿到的就是数据库中的一条数据，然后存在了一行中
        String words = value.toString();
        List<String> list = new ArrayList<>();
        int flag = 0;
        Pattern p = Pattern.compile("[^\\|]+");
        Matcher matcher = p.matcher(words);
        while (matcher.find()) {
            if (matcher.group().length() == 0 || matcher.group().equals("[]")) {
                flag = 1;
                break;
            }
            list.add(matcher.group());
        }

        if (flag == 0){
            list.clear();
            flag=0;
        }
        // 城市
        String city = list.get(1);
        // 工作
        String job  = list.get(8);
        // 福利信息
        String welfare = list.get(10);
        // 由于福利信息是由多个字符串组成的，有指定的分隔符，所以设置
        Pattern pattern1 = Pattern.compile("\'(.*?)\'"); // 利用Pattern对象指定正则
        Matcher matcher1 = pattern1.matcher(welfare); // 利用Matcher进行匹配
        // 循环依次去取数据，所取到的数据就是你要取到的数据
        String str = "";
        int flag1 = 0;
        while (matcher1.find()){
            // 根据标识符将一个一个筛出来的数据放到list中
            String temp = matcher1.group();
            // 对数据中不符合规范的进行清洗
            temp = temp.replace("\'","");
            flag++;
            if (flag==5)
                str = str + temp;
            else
                str = temp + "|" + str;
        }
        // 指定工作和城市
        if (job.equals("嵌入式软件工程师") && city.equals("北京"))
            context.write(new Text(),new Text(str));
    }
}
