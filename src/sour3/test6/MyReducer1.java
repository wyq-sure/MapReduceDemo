package sour3.test6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.jasper.tagplugins.jstl.core.ForEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MyReducer1 extends Reducer<Pojo,Text,Text,Text> {
    // 注意这里的key是使用的Pojo实体类，这样就可以利用key的排序结合在实体类中指定的排序方式实现排序了
    // 注意pojo类在之前已经将所需要的数据都封装好了。
    // 之前排序的实现是使用的在Reducer过程中将数据通过遍历出来然后封装到list中，再利用集合的排序方法实现的，
    // 这样做可能会有弊端，看实际情况下怎么做比较合适把！
    @Override
    protected void reduce(Pojo key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 遍历value使用context进行传输
        for (Text value : values) {
            context.write(value,new Text(key.toString()));
        }

    }
}
