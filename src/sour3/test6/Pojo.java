package sour3.test6;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 要求封装几个属性的值实现排序，在pojo类中就写几个属性
 * 要实现排序，需要实现WritableComparable接口，重写其中的三个方法，
 * 分别是：实现序列化、反序列化、指定用那个字段进行排序
 * 这里排序的时候只是根据整型的数据做差实现的。
 */
public class Pojo implements WritableComparable<Pojo> {
//    int id;
//    String hotel;
//    String province;
//    String city;
//    double price;
//    int count_room;
//    int count_hotel;
    double grade;
    int comment;
    int star;
//    String type;
//    int check;
//    String check_time;
//    int order_status;

    public Pojo() {
    }

    public Pojo(double grade, int comment, int star) {
        this.grade = grade;
        this.comment = comment;
        this.star = star;
    }

    @Override
    public int compareTo(Pojo o) {
       /*
       // 如果先指定按评分数进行
       int code = (int) (o.grade - this.grade);
        if (code == 0)*/
       //
       // 这里指定的是按评论数进行排名，
       int  code = o.comment - this.comment;
       return code;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(grade);
        out.writeInt(comment);
        out.writeInt(star);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        grade = in.readDouble();
        comment = in.readInt();
        star = in.readInt();

    }

    @Override
    public String toString() {
        /*return id+","+hotel+","+province+","+city+","+price+","+count_room+","+count_hotel+","+grade
                +","+comment+","+star+","+check+","+check_time+","+order_status;*/
        return grade +","+comment+","+star;
    }
}
