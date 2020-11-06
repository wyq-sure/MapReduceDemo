package sour2.demo.Mysql2Hdfs;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
/**
 * 实现DBWritable
 *
 * TblsWritable需要向mysql中写入数据
 */
public class Pojo implements Writable, DBWritable {
    String tbl_name;
    int tbl_age;

    public Pojo() {
    }

    public Pojo(String name, int age) {
        this.tbl_name = name;
        this.tbl_age = age;
    }

    /**
     * 这两个方法是对应这DBWritable接口。readFiles方法负责从结果集中读取数据库数据（注意ResultSet的下标是从1开始的），
     * 一次读取查询SQL中筛选的某一列；
     * Write方法负责将数据写入到数据库，将每一行的每一列依次写入
     */
    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.tbl_name);
        statement.setInt(2, this.tbl_age);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.tbl_name = resultSet.getString(1);
        this.tbl_age = resultSet.getInt(2);
    }

    /**
     * 以下两个是重写的Writable接口：序列化和反序列化
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.tbl_name);
        out.writeInt(this.tbl_age);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tbl_name = in.readUTF();
        this.tbl_age = in.readInt();
    }
    // 指定输出到hdfs文件的输出样式和指定数据从数据中读出来之后指定什么方式分割字段
    public String toString() {
        return this.tbl_name + "," + this.tbl_age;
    }
}
