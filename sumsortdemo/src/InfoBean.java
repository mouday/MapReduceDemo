import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义数据结构，用于传递数据，和排序输出
 * 账号的收入、支出、盈余
 */
public class InfoBean implements WritableComparable<InfoBean> {

    private String account;
    private double income;
    private double cost;
    private double surplus;


    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public double getIncome() {
        return income;
    }

    public void setIncome(double income) {
        this.income = income;
    }

    public double getCost() {
        return cost;
    }

    @Override
    public String toString() {
        return income + "\t"+ cost + "\t" +surplus;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public double getSurplus() {
        return surplus;
    }

    public void setSurplus(double surplus) {
        this.surplus = surplus;
    }

    //  使用set，用于优化内存，避免new很多对象消耗内存
    public void set(String account, double income, double cost) {
        this.account = account;
        this.income = income;
        this.cost = cost;
        this.surplus = income - cost;
    }

    // 比较排序，map输出中的key会自动被排序
    @Override
    public int compareTo(InfoBean other) {
        return this.surplus > other.surplus ? -1 : 1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.account);
        dataOutput.writeDouble(this.income);
        dataOutput.writeDouble(this.cost);
        dataOutput.writeDouble(this.surplus);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.account = dataInput.readUTF();
        this.income = dataInput.readDouble();
        this.cost = dataInput.readDouble();
        this.surplus = dataInput.readDouble();
    }
}
