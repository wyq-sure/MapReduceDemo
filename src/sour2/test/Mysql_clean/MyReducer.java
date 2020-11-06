package sour2.test.Mysql_clean;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MyReducer extends Reducer<Text, Text,Pojo, NullWritable> {
    List<Pojo> list = new ArrayList<>();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 从map中传递过来的数据是封装在key中的
        String words = key.toString();
        String[] split = words.split("\\|");
        // 将所有的数据取出来
        int id = Integer.parseInt(split[0]); // 编号
        String city = split[1]; //城市
        String company = split[2]; // 公司名
        String companySize = split[3];// 公司人数
        String companyType = split[4];// 公司性质
        String edulevel = split[5];// 学历要求
        String emplType = split[6];// 全职/兼职
        String extractSkillTag = split[7];// 当前岗位需要的技能
        String jobName = split[8];// 岗位名称
        String salary = split[9];// 薪资
        String welfare = split[10];// 福利
        String workingExp = split[11];//工作年限要求
        String addtime = split[12]; // 添加时间
        String numbers = split[13]; // 招聘人数

        // 将所有的值都封装到实体类中
        Pojo pojo = new Pojo(id,city,company,companySize,companyType,edulevel,emplType,extractSkillTag,jobName,
                salary,welfare,workingExp,addtime,numbers);

        // 将实体类添加到集合中，用于之后的排序
        list.add(pojo);
    }

    /*
        将排序的代码写在Reducer中的cleanup()方法中，利用集合的排序方法sort()对所有的数据进行排序！
        另外自定义排序要建立在实体类(需要实现接口:WritableComparable<Pojo>)中，所以集合中的属性应该是实体类对象！
        最后通过遍历集合将每个实体类对象写到context中！
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 对实体类进行排序
        Collections.sort(list);
        // 遍历集合，将集合中数据写到context中
        for (Pojo val : list) {
            context.write(val,NullWritable.get());
        }
    }
}
