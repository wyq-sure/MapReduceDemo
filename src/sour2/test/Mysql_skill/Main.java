package sour2.test.Mysql_skill;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 分析各知识技能在某个招聘岗位能力需求中的占比情况！
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 指定与hdfs连接
        conf.set("fs.defaultFS","hdfs://lky01:9000");
        // 数据库连接对象，
        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver",
                "jdbc:mysql://lky01:3306/migrate","root","123456");

        // 新建一个任务对象
        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        // 设置Mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置Reducer输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // 设置数据的读入的格式
        job.setInputFormatClass(DBInputFormat.class);
        DBInputFormat.setInput(job, sourcePojo.class,"select * from zhaopin","select count(1) from zhaopin");

        // 设置数据的输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置与Mysql数据连接的jar的位置(当前的路径是自己在hdfs中创建的，并且上传了对应版本的jar包)
        job.addArchiveToClassPath(new Path("hdfs://lky01:9000/lib/mysql/mysql-connector-java-5.1.39.jar"));

        // 设置输出路径到hdfs
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path("/output_wang_skill");
        if (fs.exists(outPath))
            fs.delete(outPath,true);
        FileOutputFormat.setOutputPath(job,outPath);
        // 提交任务等待完成、
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
