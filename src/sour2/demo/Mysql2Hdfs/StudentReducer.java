package sour2.demo.Mysql2Hdfs;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StudentReducer extends Reducer<Text,Text,Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value:
                values) {
            context.write(value,NullWritable.get());
        }
    }
}
