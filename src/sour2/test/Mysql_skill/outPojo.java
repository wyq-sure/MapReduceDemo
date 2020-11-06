package sour2.test.Mysql_skill;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 任何用作键来使用的类都应该实现WritableComparable接口
public class outPojo implements WritableComparable<outPojo> {
    String skill;
    int count;

    public outPojo() {
    }

    public outPojo(String skill, int count) {
        this.skill = skill;
        this.count = count;
    }

    public String getSkill() {
        return skill;
    }

    public void setSkill(String skill) {
        this.skill = skill;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(skill);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.skill = in.readUTF();
        this.count = in.readInt();
    }

    @Override
    public int compareTo(outPojo o) {
        // 按数量降序排列
        return o.getCount() - this.count;
    }
    // 控制当前对象被传递的时候，输出的格式，可以全部输出也可以只输出一部分（这里只是输出了其中的一个字段）
    // 实际上这样就可以在Reducer过程中输出的时候选择类型为当前实体类类型。
    // 也就是说如果要输出，多个值的时候，也可以封装在这样一个实体类中然后一起输出，不过就是得实现序列化（Writable）接口
    @Override
    public String toString() {
        return String.valueOf(count);
    }
}
