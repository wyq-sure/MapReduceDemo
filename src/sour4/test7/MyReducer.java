package sour4.test7;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

public class MyReducer extends Reducer<Text,Text,Text, Text> {

    // 根据原始数据中数字的格式，对应不同的属性值，将映射关系存储在map中
    Map<Integer,String> map = new HashedMap();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 需要将结果传递到context中的数据定义在循环外
        int sum_un =0;
        int sum = 0;
        int season = 0;
        /*
        错误整理：如果年龄放在key中，会输出多次的你在context中指定的字段
         */
        // 将要判断的数据放在迭代器中进行判断，
        for (Text value: values) {
            String val = value.toString();
            String[] vals = val.split("\\,");
            int promotion = Integer.parseInt(vals[0]);
            int price = Integer.parseInt(vals[1]);
            int number = Integer.parseInt(vals[2]);
            season = Integer.parseInt(vals[3]);
            map.put(0,"四季型商品");
            map.put(1,"春季型商品");
            map.put(2,"夏季型商品");
            map.put(3,"秋季型商品");
            map.put(4,"冬季型商品");
            // 根据是否促销计算出对应的销售额
            if (promotion==0){
                sum_un+=price*number;
            }else {
                sum+=price*number;
            }
        }
        // 将用于分组的属性字段设置为key，然后将其他属性字段封装在value中，然后输出到文件中
        context.write(new Text("分类:"+key),new Text("\n"+"未促销时的销售额:"+sum_un+"\n"
                +"促销时的销售额:"+sum+"\n"
                +"商品类型:"+map.get(season)));
    }

}
