import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 继承Mapper 实现map计算
 * 传递的参数需要实现序列化，通过网络传输
 */
public class MapDemo extends Mapper<LongWritable, Text, Text, LongWritable>{

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        // 接收数据
        String line = value.toString();
        // 切分单词
        String[] words = line.split(" ");

        // 将每个单词转为数字
       for(String word: words)
       {
           context.write(new Text(word), new LongWritable(1));
       }
    }
}
