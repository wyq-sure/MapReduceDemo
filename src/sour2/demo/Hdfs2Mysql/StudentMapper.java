package sour2.demo.Hdfs2Mysql;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class StudentMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
        // 其实在这里可以对文本做一些预处理，然后指定好分割方式传到Reducer
        context.write(key, value);
    }
}