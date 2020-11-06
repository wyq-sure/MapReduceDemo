package sour3.test5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 统计符合要求的高端酒店的相关信息
 */
public class Main {

    public static void main(String[] args) throws Exception {
        // 获取配置对象
        Configuration conf = new Configuration();
        // 获取实际工作的job1对象
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(Main.class);
        job1.setMapperClass(MyMapper1.class);
//        job1.setReducerClass(MyReducer1.class);
        // 设置Mapper1和Reducer1的输出格式
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
//        job1.setOutputKeyClass(Text.class);
//        job1.setOutputValueClass(Text.class);
        // 设置输入输出路径
        Path inPath = new Path("C:\\Users\\Admin\\Desktop\\数据源\\3\\text2.csv");
        Path outPath = new Path("C:\\Users\\Admin\\Desktop\\数据源\\3\\out_test5_01");
        // 控制输出路径一直不为空
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath))
            fs.delete(outPath,true);
        // 将job对象和路径信息传给输入输出格式化对象
        FileInputFormat.addInputPath(job1,inPath);
        FileOutputFormat.setOutputPath(job1,outPath);
        // 提交任务等待完成,如果任务正确完成之后的返回值应该是0
        int code = job1.waitForCompletion(true) ? 0 : 1;
        // 根据返回值，如果正常完成即返回值是0的是，执行第二个NR任务。
        // 也就是实现了当第一个任务完后再执行第二个MR任务
       /* if (code == 0){
            // 获取配置对象
            Configuration conf2 = new Configuration();
            // 获取实际工作的job1对象
            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(sour3.test2.Main.class);
            job2.setMapperClass(MyMapper2.class);
            job2.setReducerClass(MyReducer2.class);
            // 设置Mapper2和Reducer2的输出格式
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            // 设置输入、输出路径
            Path inPath2 = new Path("C:\\Users\\Admin\\Desktop\\数据源\\3\\out_01");
            Path outPath2 = new Path("C:\\Users\\Admin\\Desktop\\数据源\\3\\out2_01");
            FileSystem fs2 = FileSystem.get(conf2);
            if (fs2.exists(outPath2)){
                fs.delete(outPath2,true);
            }
            FileInputFormat.addInputPath(job2,inPath2);
            FileOutputFormat.setOutputPath(job2,outPath2);
            // 提交任务等待完成，然后也是赋值给保存第一个MR的结果的变量
            code = job2.waitForCompletion(true) ? 0 : 1;
        }*/
        // 将最后的code进行返回。注意这里的返回值是code！
        System.exit(code);
    }
}
