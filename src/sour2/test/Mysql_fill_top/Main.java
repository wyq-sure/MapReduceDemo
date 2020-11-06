package sour2.test.Mysql_fill_top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 使用众数填充缺失值（所有工资中出现最多的那个工资作为填充）
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://lky01:9000");

        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver",
                "jdbc:mysql://lky01:3306/migrate","root","123456");

        // 新建一个任务对象
        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        // 设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置数据的读入的格式
        job.setInputFormatClass(DBInputFormat.class);
        DBInputFormat.setInput(job, Pojo.class,"select * from testavg","select count(1) from testavg");

        // 设置数据的输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置与Mysql数据连接的jar的位置
        job.addArchiveToClassPath(new Path("hdfs://lky01:9000/lib/mysql/mysql-connector-java-5.1.39.jar"));

        // 设置输出路径到hdfs
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path("/output_wang_fill_top");
        if (fs.exists(outPath))
            fs.delete(outPath,true);



        FileOutputFormat.setOutputPath(job,outPath);
        // 提交任务等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
