package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 计算时间差
 * 判断测试时间是否在一个时间段内
 */
public class timeGap {
    public static void main(String[] args) throws ParseException {
        // 取一个时间段:可以采用计算时间的毫秒数来进行截取
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date start = sdf.parse("2016-07-11");
        Date test = sdf.parse("2016-09-11");
        Date end = sdf.parse("2017-08-12");
        long startTime = start.getTime();
        long testTime = test.getTime();
        long endTime = end.getTime();
        // 之间相差的时间戳
        long day = endTime - startTime;
        // 将之间相差的时间戳可以转成之间相差的天数
        long day3 = day / (24 * 60 * 60 * 1000);
        System.out.println(day);
        System.out.println(day3);
        // 判断测试时间是否是在当前控制的当前时间段内
        if (testTime > startTime && startTime < endTime) {
            System.out.println("说明测试时间在当前时间段内！");
        }
    }
}
/*
输出结果：
34300800000
397
说明测试时间在当前时间段内！
 */
