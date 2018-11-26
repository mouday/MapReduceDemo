package hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFS操作，需要导入common和hdsf文件夹下的jar
 */
public class HDSFDemo {

    private FileSystem fs = null;

    HDSFDemo() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();

        fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "root");

    }

    /**
     * 下载文件
     *
     * @throws IOException
     */
    public void download() throws IOException {
        InputStream in = fs.open(new Path("/java"));
        OutputStream out = new FileOutputStream("/Users/qmp/Desktop");
        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**
     * 上传文件
     *
     * @throws IOException
     */
    public void upload() throws IOException {
        System.out.println("开始上传...");
        InputStream in = new FileInputStream("/Users/qmp/Desktop/compare.py");
        OutputStream out = this.fs.create(new Path("/compare.py"));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**
     * 删除文件
     *
     * @throws IOException
     */
    public void delete() throws IOException {
        boolean ret = this.fs.delete(new Path("/java"), true);
        System.out.println("文件删除结果：" + ret);
    }

    /**
     * 创建文件夹
     *
     * @throws IOException
     */
    public void mkdir() throws IOException {
        boolean ret = this.fs.mkdirs(new Path("/temp"));
        System.out.println("文件创建成功：" + ret);
    }


    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        HDSFDemo hdfs = new HDSFDemo();
        hdfs.upload();


    }
}
