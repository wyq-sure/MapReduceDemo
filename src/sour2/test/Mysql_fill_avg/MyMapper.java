package sour2.test.Mysql_fill_avg;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyMapper extends Mapper<Object, Pojo, Text,DoubleWritable> {

    /**
     * 当前数据的来源是Mysql，数据封装在了实体类中；
     * 如果数据是来自于文本或者hdfs，也要将数据封装在实体中，这样才可以使用getter方法获得某个属性在特定条件下的所有值
     */
    // 定义一个全局的list变量，保存;然后在cleanup方法中调用的时候就是一个数组的
    List<Double> list_salary = new ArrayList<>();
    List<String> list_first = new ArrayList<>();
    List<String> list_old = new ArrayList<>();
    List<String> list_null = new ArrayList<>();
    Double[] array;


    @Override
    protected void map(Object key, Pojo value, Context context) throws IOException, InterruptedException {
        // 这里拿到的就是数据库中的一条数据，然后存在了一行中
        String words = value.toString();
        String[] string = words.split("\\|");
        String id = string[0];
        String city = string[1];
        String jobName = string[2];
        double salary = Double.parseDouble(string[3].trim());
        // 重新拼接字符串作为context的Key传到Reducer中
        String str = id + "|" + city + "|" + jobName + "|" + salary;
        // 将原本的字段封装在
        list_first.add(str);

        list_salary.add(salary);
        array = list_salary.toArray(new Double[0]);

        list_old.add(jobName + "|" + salary);

        if (salary == 0.0) {
            list_null.add(jobName + "|" + salary);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int count = 0;
        double sum = 0.0;
        double avg = 0.0;
        for (String ls_null : list_null) {
            // 对元素进行分割
            String[] name_salary_null = ls_null.split("\\|");
            // 取出name
            String name_null = name_salary_null[0];

            for (int i = 0; i < list_old.size(); i++) {
                // 每一行的元素
                String ls = list_old.get(i);
                // 对元素进行分割
                String[] name_salary = ls.split("\\|");
                // 取出name、salary
                String name_old = name_salary[0];
                double salary_old = Double.parseDouble(name_salary[1]);

                if (name_old.equals(name_null)) {
                    sum += salary_old;
                    count++;
                }
                // 求出平均值
                avg = sum / count;
            }
            for (String str : list_first) {
                context.write(new Text(str), new DoubleWritable(avg));
            }
        }
    }
}