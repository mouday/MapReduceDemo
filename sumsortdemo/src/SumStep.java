import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 求和计算步骤，计算出每个账号的收入、支出、盈余总额
 * hadoop jar sumsortdemo.jar SumStep /infos.txt /info-out
 */
public class SumStep {

    public static class SumMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
        private Text text = new Text();
        private InfoBean infoBean = new InfoBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            String account = fields[0];
            double income = Double.parseDouble(fields[1]);
            double cost = Double.parseDouble(fields[2]);

            text.set(account);
            infoBean.set(account, income, cost);

            context.write(text, infoBean);

        }
    }

    public static class SumReducer extends Reducer<Text, InfoBean, Text, InfoBean> {
        private InfoBean infoBean = new InfoBean();

        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            double totalIncome = 0;
            double totalCost = 0;

            for (InfoBean info : values) {
                totalIncome += info.getIncome();
                totalCost += info.getCost();
            }

            infoBean.set("", totalIncome, totalCost);
            context.write(key, infoBean);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJarByClass(SumStep.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);

        job.waitForCompletion(true);

    }
}
