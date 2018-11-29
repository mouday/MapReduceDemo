import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计单词个数
 * 运行：hadoop jar hdfsdemo.jar
 * 根据实际路径指定输入输出文件 hadoop jar hdfsdemo.jar /word.txt /out
 */
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 构建Job对象
        Job job = Job.getInstance(new Configuration());

        // 注意：main方法所在类
        job.setJarByClass(WordCount.class);

        // 设置输入文件路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置Mapper属性
        job.setMapperClass(MapDemo.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置Reducer属性
        job.setReducerClass(ReduceDemo.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置输出文件路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交任务
        job.waitForCompletion(true);

    }
}
