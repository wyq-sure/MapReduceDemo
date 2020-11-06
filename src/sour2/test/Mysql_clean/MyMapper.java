package sour2.test.Mysql_clean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

public class MyMapper extends Mapper<Object, Pojo, Text, Text> {


    @Override
    protected void map(Object key, Pojo value, Context context) throws IOException, InterruptedException {
        // 这里拿到的就是数据库中的一条数据，然后存在了一行中
        String words = value.toString();
        // 定义一个数组，用于将数据分割后可以拿个每一个数组元素
        String[] spilt = words.split("\\|");
        List<String> list = new ArrayList<>();
        int flag = 0;
        String city = spilt[1];
        String extractSkillTag = spilt[7];
        String welfare = spilt[10];
        String sarly = spilt[9];
        String numbers = spilt[13];
        String addtime = spilt[12];
        if (extractSkillTag.equals("[]") || welfare.equals("[]")){
            flag+=1;
        }

        if (city.equals("")){
            spilt[1] = "默认城市";
        }

        if (sarly.equals("")){
            spilt[9] = "默认薪资";
        }else if (!Pattern.matches("(.*)K",sarly)){
            int sa = Integer.parseInt(sarly);
            spilt[9] = sa/1000+"K";
        }

        if (Pattern.matches("(.*)人",numbers)) {
            int number1 = Integer.parseInt(numbers.substring(0,numbers.length() - 1));
            if (number1 <= 0)
                number1=99999;
            spilt[13] = number1 + "人";
        }else {
            int number2 = Integer.parseInt(numbers);
            if (number2 <= 0)
                number2=88888;
            spilt[13] = number2 + "人";
        }

        // 对时间进行格式化处理
        // 定义一个时间类型的数据格式
        Date date = null;
        try {
            // 先设置一个格式
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy,MM,dd,HH:mm:ss");
            // 预格式化的字段作为参数直接传递!!!
            date = sdf.parse(addtime);
            // 再设置一个你想格式化成的格式
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
            // 传入之前要设置的Date的对象
//          System.out.println(sdf2.format(data));
            spilt[12] = sdf2.format(date);
        } catch (ParseException e) {
           try {
               // 先设置一个格式
               SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
               date = sdf.parse(addtime);
               // 再设置一个你想格式化成的格式
               SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
               // 传入之前要设置的Date的对象
//            System.out.println(sdf2.format(data));
               spilt[12] = sdf2.format(date);
           }catch (ParseException e1){
               try {
                   // 先设置一个格式
                   SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH:mm:ss");
                   date = sdf.parse(addtime);
                   // 再设置一个你想格式化成的格式
                   SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
                   // 传入之前要设置的Date的对象
//            System.out.println(sdf2.format(data));
                   spilt[12] = sdf2.format(date);
               }catch (ParseException e2){
                   try {
                       // 先设置一个格式
                       SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                       date = sdf.parse(addtime);
                       // 再设置一个你想格式化成的格式
                       SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
                       // 传入之前要设置的Date的对象
//            System.out.println(sdf2.format(data));
                       spilt[12] = sdf2.format(date);
                   }catch (ParseException e3){
                       try {
                           // 先设置一个格式
                           SimpleDateFormat sdf = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
                           date = sdf.parse(addtime);
                           // 再设置一个你想格式化成的格式
                           SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
                           // 传入之前要设置的Date的对象
//            System.out.println(sdf2.format(data));
                           spilt[12] = sdf2.format(date);
                       }catch (ParseException e4){
                           // 格式化时间戳
                           SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
                           Date date2 = new Date(Long.valueOf(addtime));
                           spilt[12] = sdf2.format(date2);
                       }
                   }
               }
           }
        }



        // 由于一些缺失值较少的时候，要采用删除这一行记录的方法，所以要将值保存在list集合中
        for (int i=0;i<spilt.length;i++){
            list.add(spilt[i]);
        }

        if (flag == 0) {
            mp(key, list, context);
        } else {
            list.clear();
            flag = 0;
        }
    }

    /**
     * 这里要处理的还是一行数据，由于是存在list中的，所以在这里要遍历一下集合中的元素重新进行一次拼接
     * 最后将拼接好的一整行数据直接写到context
     */
    private void mp(Object key, List<String> list, Context context) throws IOException, InterruptedException {
        // 当前string中存的仍然是一行的数据
        String string = "";
        int count = 0;
        for (String ls : list) {
            count+=1;
            if (count == 14)
                string = string + ls;
            else
                string = string + ls + '|';
        }
        // 将拼接之后的数据写到context中
       context.write(new Text(string), new Text());
    }
}
