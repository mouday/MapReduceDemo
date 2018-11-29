import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * 继承Reducer，实现reduce计算
 */
public class ReduceDemo extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
    {
        // 定义计数器
        long count = 0;

        // 统计
        for (LongWritable counter : values)
        {
            count += counter.get();
        }

        // 输出结果
        context.write(key, new LongWritable(count));
    }
}
