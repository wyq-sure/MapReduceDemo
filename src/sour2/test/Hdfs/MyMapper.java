package sour2.test.Hdfs;

import sour2.test.Hdfs_welfare.Pojo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Pojo, Text, NullWritable> {
    @Override
    protected void map(Object key, Pojo value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
