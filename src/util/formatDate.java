package util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 对不符合规范的时间进行格式化
 * 当有多个不符合规范的时间传递的时候，可以采用try-catch嵌套的结构去实现不同的判断和转换
 * 因为sdf.parse()方法在转换时间格式是会抛出异常
 */
public class formatDate {
    public static void main(String[] args) throws Exception {


        // 先设置一个未格式化之前的时间格式
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm");
        // 传入要修改的时间
        Date data = sdf.parse("5/28/2013 12:23");

        // 再设置一个你想格式化成的格式
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
        // 传入之前要设置的Date的对象
        System.out.println(sdf2.format(data));
        // 测试方法
       // testStringToDate();
        }

    private static void testStringToDate() {
        String s = "2017-05-25";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;

        try {
            date = format.parse(s);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        System.out.println(date);
    }
}



        /*
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MMM-dd");
        String formatStr =formatter.format(new Date());
        System.out.println(formatStr); //2017-九月-15 13:17:08:355

        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
        String formatStr2 =formatter2.format(new Date());
        System.out.println(formatStr2); //2017-09-15 13:18:44:672

        SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy-M-dd");
        String formatStr3 =formatter3.format(new Date());
        System.out.println(formatStr3); //2017-09-15 13:18:44:672


         // 格式化时间戳
         SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
               Date date2 = new Date(Long.valueOf(addtime));
               spilt[12] = sdf2.format(date2);

        */

