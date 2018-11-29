import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 组装任务到Job
 * 打包成普通jar包，
 * 上传至hadoop：hadoop fs -put data.dat /data.dat
 * 运行任务：hadoop jar hdfsdemo.jar DataCount /data.dat /dataout
 */
public class DataCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 实例化Job对象
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJarByClass(DataBean.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置mapper和输出数据类型
        job.setMapperClass(MyMapper.class);

        // k2, v2 和 k3, v3一致可以省略设置
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean.class);

        // 设置reducer和输出数据类型
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);

        // 设置分区,数据分流
        job.setPartitionerClass(MyPartitioner.class);

        // 设置启动Reducer数量
        job.setNumReduceTasks(Integer.parseInt(args[2]));

        // 启动任务
        job.waitForCompletion(true);

        //运行 hadoop jar datacount.jar DataCount /data.dat /data-4 4

    }
}
