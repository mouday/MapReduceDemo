package datacount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 实现Reducer 统计数据
 */

public class MyReducer extends Reducer<Text, DataBean, Text, DataBean> {
    @Override
    protected void reduce(Text key, Iterable<DataBean> values, Context context) throws IOException, InterruptedException {
        Long upSum = 0L;
        Long downSum = 0L;

        for (DataBean bean : values) {
            upSum += bean.getUpPayload();
            downSum += bean.getDownPayload();
        }
        context.write(key, new DataBean("", upSum, downSum));
    }
}
