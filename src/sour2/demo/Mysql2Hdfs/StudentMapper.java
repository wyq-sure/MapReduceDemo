package sour2.demo.Mysql2Hdfs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * Mapper类 数据的传入方式是一个实体类
 */
public  class StudentMapper extends Mapper<Object, Pojo, Text, Text> {
    @Override
    protected void map(Object key, Pojo value, Context context) throws IOException, InterruptedException {
        context.write(new Text(),new Text(value.toString()));
    }
}