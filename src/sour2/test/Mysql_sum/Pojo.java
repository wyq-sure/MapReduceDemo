package sour2.test.Mysql_sum;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Pojo implements Writable, DBWritable {
    int id; // 编号
    String city; //城市
    String company; // 公司名
    String companySize;// 公司人数
    String companyType;// 公司性质
    String edulevel;// 学历要求
    String emplType;// 全职/兼职
    String extractSkillTag;// 当前岗位需要的技能
    String jobName;// 岗位名称
    String salary;// 薪资
    String welfare;// 福利
    String workingExp;//工作年限要求

    public Pojo() {
    }

    public Pojo(int id, String city, String company, String companySize, String companyType, String edulevel, String emplType, String extractSkillTag, String jobName, String salary, String welfare, String workingExp) {
        this.id = id;
        this.city = city;
        this.company = company;
        this.companySize = companySize;
        this.companyType = companyType;
        this.edulevel = edulevel;
        this.emplType = emplType;
        this.extractSkillTag = extractSkillTag;
        this.jobName = jobName;
        this.salary = salary;
        this.welfare = welfare;
        this.workingExp = workingExp;
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

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getCompanySize() {
        return companySize;
    }

    public void setCompanySize(String companySize) {
        this.companySize = companySize;
    }

    public String getCompanyType() {
        return companyType;
    }

    public void setCompanyType(String companyType) {
        this.companyType = companyType;
    }

    public String getEdulevel() {
        return edulevel;
    }

    public void setEdulevel(String edulevel) {
        this.edulevel = edulevel;
    }

    public String getEmplType() {
        return emplType;
    }

    public void setEmplType(String emplType) {
        this.emplType = emplType;
    }

    public String getExtractSkillTag() {
        return extractSkillTag;
    }

    public void setExtractSkillTag(String extractSkillTag) {
        this.extractSkillTag = extractSkillTag;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getSalary() {
        return salary;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }

    public String getWelfare() {
        return welfare;
    }

    public void setWelfare(String welfare) {
        this.welfare = welfare;
    }

    public String getWorkingExp() {
        return workingExp;
    }

    public void setWorkingExp(String workingExp) {
        this.workingExp = workingExp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.city);
        out.writeUTF(this.company);
        out.writeUTF(this.companySize);
        out.writeUTF(this.companyType);
        out.writeUTF(this.edulevel);
        out.writeUTF(this.emplType);
        out.writeUTF(this.extractSkillTag);
        out.writeUTF(this.jobName);
        out.writeUTF(this.salary);
        out.writeUTF(this.welfare);
        out.writeUTF(this.workingExp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.city = in.readUTF();
        this.company = in.readUTF();
        this.companySize = in.readUTF();
        this.companyType = in.readUTF();
        this.edulevel = in.readUTF();
        this.emplType = in.readUTF();
        this.extractSkillTag = in.readUTF();
        this.jobName = in.readUTF();
        this.salary = in.readUTF();
        this.welfare = in.readUTF();
        this.workingExp = in.readUTF();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1,this.id);
        statement.setString(2,this.city);
        statement.setString(3,this.company);
        statement.setString(4,this.companySize);
        statement.setString(5,this.companyType);
        statement.setString(6,this.edulevel);
        statement.setString(7,this.emplType);
        statement.setString(8,this.extractSkillTag);
        statement.setString(9,this.jobName);
        statement.setString(10,this.salary);
        statement.setString(11,this.welfare);
        statement.setString(12,this.workingExp);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getInt(1);
        this.city = resultSet.getString(2);
        this.company = resultSet.getString(3);
        this.companySize = resultSet.getString(4);
        this.companyType = resultSet.getString(5);
        this.edulevel = resultSet.getString(6);
        this.emplType = resultSet.getString(7);
        this.extractSkillTag = resultSet.getString(8);
        this.jobName = resultSet.getString(9);
        this.salary = resultSet.getString(10);
        this.welfare = resultSet.getString(11);
        this.workingExp = resultSet.getString(12);
    }

    @Override
    public String toString() {
        return id + "|"+city+ "|"+company+ "|"+companySize+ "|"+companyType+"|"+edulevel+ "|"+emplType+ "|"+extractSkillTag+ "|"+jobName+ "|"+salary+ "|"+welfare+ "|"+workingExp;
    }
}
