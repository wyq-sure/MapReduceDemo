package sour2.test.Hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 与数据源是Mysql的不同之处就是设置路径
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://lky01:9000");

        // 新建一个任务对象
        Job job = Job.getInstance(conf);
        job.setJarByClass(sour2.test.Hdfs_welfare.Main.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        // 设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置数据的读入的格式
        job.setInputFormatClass(TextInputFormat.class);

        // 设置数据的输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输入路径为hdfs
        Path inPath = new Path("");
        FileInputFormat.addInputPath(job,inPath);

        // 设置输出路径到hdfs
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path("/output_wang_welfare");
        if (fs.exists(outPath))
            fs.delete(outPath,true);
        FileOutputFormat.setOutputPath(job,outPath);
        // 提交任务等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
