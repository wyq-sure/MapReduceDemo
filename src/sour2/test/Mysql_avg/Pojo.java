package sour2.test.Mysql_avg;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 首先想使用实体类进行数据的封装，就必须实现Writable接口
 * 然后要对数据库进行操作还要重写一个DBWritable接口
 * 并且重写其中的两个方法
 */
public class Pojo implements Writable, DBWritable {
    int id; // 编号
    String city; //城市
    String jobName;// 岗位名称
    double salary;// 薪资

    // 基本思路：无参、有参构造，getter、setter方法
    public Pojo() {
    }

    public Pojo(int id, String city, String jobName, Float salary) {
        this.id = id;
        this.city = city;
        this.jobName = jobName;
        this.salary = salary;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }



    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.city);
        out.writeUTF(this.jobName);
        out.writeDouble(this.salary);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.city = in.readUTF();
        this.jobName = in.readUTF();
        this.salary = in.readDouble();

    }

    // 注意这里的索引值是从1开始的
    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1,this.id);
        statement.setString(2,this.city);
        statement.setString(3,this.jobName);
        statement.setDouble(4,this.salary);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getInt(1);
        this.city = resultSet.getString(2);
        this.jobName = resultSet.getString(3);
        this.salary = resultSet.getDouble(4);

    }
    // 为了MR读取数据，还需要指定分隔符进行数据的拼接，
    @Override
    public String toString() {
        return id + "|"+city+ "|"+jobName+ "|" +salary;
    }

}
