package datacount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现自定义数据结构，序列化和反序列化
 */
public class DataBean implements Writable {

    private String telNo;
    private Long upPayload;
    private Long downPayload;
    private Long totalPayload;

    public DataBean(){}

    public DataBean(String telNo, Long upPayload, Long downPayload) {
        this.telNo = telNo;
        this.upPayload = upPayload;
        this.downPayload = downPayload;
        this.totalPayload = upPayload + downPayload;
    }

    @Override
    public String toString() {
        return "DataBean{" +
                "telNo='" + telNo + '\'' +
                ", upPayload=" + upPayload +
                ", downPayload=" + downPayload +
                ", totalPayload=" + totalPayload +
                '}';
    }

    public String getTelNo() {
        return telNo;
    }

    public void setTelNo(String telNo) {
        this.telNo = telNo;
    }

    public Long getUpPayload() {
        return upPayload;
    }

    public void setUpPayload(Long upPayload) {
        this.upPayload = upPayload;
    }

    public Long getDownPayload() {
        return downPayload;
    }

    public void setDownPayload(Long downPayload) {
        this.downPayload = downPayload;
    }

    public Long getTotalPayload() {
        return totalPayload;
    }

    public void setTotalPayload(Long totalPayload) {
        this.totalPayload = totalPayload;
    }


    public static void main(String[] args) {

    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(telNo);
        dataOutput.writeLong(upPayload);
        dataOutput.writeLong(downPayload);
        dataOutput.writeLong(totalPayload);
    }

    /**
     * 反序列化
     * 注意：1.类型 2.顺序
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.telNo = dataInput.readUTF();
        this.upPayload = dataInput.readLong();
        this.downPayload = dataInput.readLong();
        this.totalPayload = dataInput.readLong();
    }
}
