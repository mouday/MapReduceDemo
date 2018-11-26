package datacount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 实现Mapper，逐行处理数据
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, DataBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String telNo = fields[1];
        Long up = Long.parseLong(fields[7]);
        Long down = Long.parseLong(fields[8]);
        DataBean bean = new DataBean(telNo, up, down);

        context.write(new Text(telNo), bean);
    }


}
