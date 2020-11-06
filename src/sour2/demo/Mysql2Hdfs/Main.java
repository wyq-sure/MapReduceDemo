package sour2.demo.Mysql2Hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        // 读取配置文件
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://lky01:9000");

        // 参数：数据库驱动名称，数据库URL，用户名，密码
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://lky01:3306/formatDate", "root", "123456");

        // 新建一个任务
        Job job = Job.getInstance(conf);
        // 设置主类
        job.setJarByClass(Main.class);
        // Mapper
        job.setMapperClass(StudentMapper.class);

        // Reducer
        job.setReducerClass(StudentReducer.class);

        // Map输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置数据的输入格式:从数据库中读
        job.setInputFormatClass(DBInputFormat.class);
        // 设置实体类，查询语句
        DBInputFormat.setInput(job, Pojo.class, "select * from student","select count(1) from student");

        // 输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        // 添加mysql数据库jar
        job.addArchiveToClassPath(new Path("hdfs://lky01:9000/lib/mysql/mysql-connector-java-5.1.39.jar"));

        // 输出路径到Hdfs
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path("hdfs://lky01:9000/output_wang2");
        if (fs.exists(outPath))
            fs.delete(outPath,true);
        FileOutputFormat.setOutputPath(job,outPath);

        //提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}