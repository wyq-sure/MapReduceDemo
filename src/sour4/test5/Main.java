package sour4.test5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计客户的会员卡分级情况
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://lky01:9000");// 指定数据的来源是hdfs

        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path inPath = new Path("/martdata/client.csv");
        Path outPath = new Path("/sour4/test5");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath))
            fs.delete(outPath,true);
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        // 由于这里的返回值值int，如果要指定多个MR进行工作，就可以根据返回值来进行判断，然后接着书写
        System.exit(job.waitForCompletion(true) ? 0 : 1);
       }
}
