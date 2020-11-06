package sour2.test.Mysql_skill;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MyReducer extends Reducer<Text,IntWritable, Text, DoubleWritable> {
    // 定义一个集合存输出对象，对象中的toString()方法只输出一个count
    List<outPojo> list = new ArrayList<>();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context){
        // 保存每个key的次数和的变量
        int sum = 0;
        // 依次循环遍历每一个value（值都是1），然后做和
        for (IntWritable value : values) {
            sum += value.get();
        }
        // 创建实现了排序接口的对象（当前对象有两个属性：技能点、数量），将查到的数据都封装在list中，之后再存到list集合中，排序！
        outPojo outPojo = new outPojo(key.toString(),sum);
        // 将每个待排序对象加入到list中
        list.add(outPojo);
    }

    /**
     * 当前方法只执行一次，这样实现对数据的操作不再是一组一组输出，而是全部拿到，最后过滤输出。、
     * 比如这里要求TOP-k就是一个经典的应用
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 调用list集合的排序方法
        Collections.sort(list);
        // 遍历list集合
        // 设置一个计数器
        int flag = 0;
        // 遍历集合中的每一个对象
        double sum = 0;
        String skill = null;
        int count = 0;
        for (outPojo out : list) {
            // 将对象中封装的技能点字段值利用getter取出来
             skill = out.getSkill();
            // 同理取count字段
             count = out.getCount();
            // 计数器加加
             sum++;
            // 循环依次将数据都写入到context中
            context.write(new Text(skill),new DoubleWritable(count/sum));
          /*  //根据计数器的大小控制输出前10行
            flag++;
            // 当flag等于10的时候就返回
            if (flag == 10)
                return;*/
        }

    }
}
