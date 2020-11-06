package sour2.demo.Hdfs2Mysql;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        // 读取配置文件
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://lky01:9000");

        // 参数：数据库驱动名称，数据库URL，用户名，密码
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://lky01:3306/formatDate", "root", "123456");

        // 新建一个任务对象
        Job job = Job.getInstance(conf);

        // 设置主类
        job.setJarByClass(Main.class);

        // Mapper
        job.setMapperClass(StudentMapper.class);

        // Reducer
        job.setReducerClass(StudentReducer.class);

        // mapper输出格式
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 指定输出格式
        job.setOutputKeyClass(Pojo.class);
        job.setOutputValueClass(NullWritable.class);

        // 输入路径
        FileInputFormat.addInputPath(job, new Path("hdfs://lky01:9000/input/student.txt"));
        // 输入格式，默认就是TextInputFormat
        job.setInputFormatClass( TextInputFormat.class);

        // 输出格式
        job.setOutputFormatClass(DBOutputFormat.class);
        // 输出到哪些表、字段
        DBOutputFormat.setOutput(job, "student", "name", "age");

        // 添加mysql数据库jar，这一步的基础是要先找到Mysql的驱动包，然后上传到Hdfs中
        job.addArchiveToClassPath(new Path("hdfs://lky01:9000/lib/mysql/mysql-connector-java-5.1.39.jar"));

        //提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}