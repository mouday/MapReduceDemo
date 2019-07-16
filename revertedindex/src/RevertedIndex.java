import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 计算倒排索引的过程 单词-文档矩阵
 * mapper -> combiner -> reducer
 * 明确每个过程的输入和输出 数据结构
 */
public class RevertedIndex {

    /**
     * Mapper
     * in: <1, hello world>
     * out: <hello->a.txt, 1>
     */
    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filePath = fileSplit.getPath().toString();

            for (String word : words) {
                k.set(word + "->" + filePath);
                v.set("1");
                context.write(k, v);
            }
        }
    }

    /**
     * Combiner
     * in: <hello->a.txt, [1, 1, 1]>
     * out: <helo, a.txt->1>
     */
    public static class IndexCombiner extends Reducer<Text, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (Text value : values) {
                counter += Integer.parseInt(value.toString());
            }
            String[] words = key.toString().split("->");
            String word = words[0];
            String filePath = words[1];

            k.set(word);

            v.set(filePath + "->" + counter);

            context.write(k, v);

        }
    }

    /**
     * Reducer
     * in: <helo, [a.txt->3, b.txt->2]>
     * out: <hello, a.txt->3 b.txt->2>
     */
    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value.toString());
            }
            v.set(sb.toString());

            context.write(key, v);

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJarByClass(RevertedIndex.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(IndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(IndexCombiner.class);

        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);


    }
}
