import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 排序步骤，mapper中的key会被自动排序需要实现接口 WritableComparable
 * hadoop jar sumsortdemo.jar SortStep /info-out/part-r-00000 /info-out2
 */
public class SortStep {
    public static class SortMapper extends Mapper<LongWritable, Text, InfoBean, NullWritable> {

        private InfoBean infoBean = new InfoBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            String account = fields[0];
            double income = Double.parseDouble(fields[1]);
            double cost = Double.parseDouble(fields[2]);

            infoBean.set(account, income, cost);

            context.write(infoBean, NullWritable.get());

        }
    }

    public static class SortReducer extends Reducer<InfoBean, NullWritable, Text, InfoBean> {
        private Text text = new Text();

        @Override
        protected void reduce(InfoBean infoBean, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String account = infoBean.getAccount();
            text.set(account);
            context.write(text, infoBean);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJarByClass(SortStep.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(InfoBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);

        job.waitForCompletion(true);

    }
}
